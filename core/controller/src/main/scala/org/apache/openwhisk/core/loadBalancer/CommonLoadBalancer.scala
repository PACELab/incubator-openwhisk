/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.LongAdder

import akka.actor.ActorSystem
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.collection.mutable //avs
import scala.collection.mutable.ListBuffer
//import scala.collection.immutable //avs


/**
 * Abstract class which provides common logic for all LoadBalancer implementations.
 */
abstract class CommonLoadBalancer(config: WhiskConfig,
                                  feedFactory: FeedFactory,
                                  loadFeedFactory: FeedFactory, // avs
                                  controllerInstance: ControllerInstanceId)(implicit val actorSystem: ActorSystem,
                                                                            logging: Logging,
                                                                            materializer: ActorMaterializer,
                                                                            messagingProvider: MessagingProvider)
    extends LoadBalancer {

  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  val lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val adaptiveLbConfig: AdaptiveContainerPoolBalancerConfig =
    loadConfigOrThrow[AdaptiveContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  val rrLbConfig: RoundRobinContainerPoolBalancerConfig =
    loadConfigOrThrow[RoundRobinContainerPoolBalancerConfig](ConfigKeys.loadbalancer)

  protected val invokerPool: ActorRef

  // avs --begin
  protected[loadBalancer] var curRunningActions = mutable.Map.empty[String, ActionStats] 
  var allInvokers = mutable.Map.empty[InvokerInstanceId, AdapativeInvokerStats]

  var lastUpdatedTime: Long = 0
  var scanIntervalInMS: Long = 10*1000 // 10 seconds!
  // i.e. if  I have more than (acceptableUnsafeInvokerRatio*100)% invokers which are unsafe in either of the workload types, I will upgrade an invoker..
  var acceptableUnsafeInvokerRatio = 0.75 
  // avs --end
  /** State related to invocations and throttling */
  protected[loadBalancer] val activationSlots = TrieMap[ActivationId, ActivationEntry]()
  protected[loadBalancer] val activationPromises =
    TrieMap[ActivationId, Promise[Either[ActivationId, WhiskActivation]]]()
  protected val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  protected val totalActivations = new LongAdder()
  protected val totalBlackBoxActivationMemory = new LongAdder()
  protected val totalManagedActivationMemory = new LongAdder()

  protected def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LOADBALANCER_ACTIVATIONS_INFLIGHT(controllerInstance), totalActivations.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, ""),
      totalBlackBoxActivationMemory.longValue + totalManagedActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Blackbox"),
      totalBlackBoxActivationMemory.longValue)
    MetricEmitter.emitGaugeMetric(
      LOADBALANCER_MEMORY_INFLIGHT(controllerInstance, "Managed"),
      totalManagedActivationMemory.longValue)
  }

  actorSystem.scheduler.schedule(10.seconds, 10.seconds)(emitMetrics())

  override def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue()).getOrElse(0))
  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue())

  /**
   * 2. Update local state with the activation to be executed scheduled.
   *
   * All activations are tracked in the activationSlots map. Additionally, blocking invokes
   * are tracked in the activation results map. When a result is received via activeack, it
   * will cause the result to be forwarded to the caller waiting on the result, and cancel
   * the DB poll which is also trying to do the same.
   */
  protected def setupActivation(msg: ActivationMessage,
                                action: ExecutableWhiskActionMetaData,
                                instance: InvokerInstanceId): Future[Either[ActivationId, WhiskActivation]] = {

    totalActivations.increment()
    val isBlackboxInvocation = action.exec.pull
    val totalActivationMemory =
      if (isBlackboxInvocation) totalBlackBoxActivationMemory else totalManagedActivationMemory
    totalActivationMemory.add(action.limits.memory.megabytes)

    activationsPerNamespace.getOrElseUpdate(msg.user.namespace.uuid, new LongAdder()).increment()

    // Timeout is a multiple of the configured maximum action duration. The minimum timeout is the configured standard
    // value for action durations to avoid too tight timeouts.
    // Timeouts in general are diluted by a configurable factor. In essence this factor controls how much slack you want
    // to allow in your topics before you start reporting failed activations.
    val timeout = (action.limits.timeout.duration.max(TimeLimit.STD_DURATION) * lbConfig.timeoutFactor) + 1.minute

    val resultPromise = if (msg.blocking) {
      activationPromises.getOrElseUpdate(msg.activationId, Promise[Either[ActivationId, WhiskActivation]]()).future
    } else Future.successful(Left(msg.activationId))

    // Install a timeout handler for the catastrophic case where an active ack is not received at all
    // (because say an invoker is down completely, or the connection to the message bus is disrupted) or when
    // the active ack is significantly delayed (possibly dues to long queues but the subject should not be penalized);
    // in this case, if the activation handler is still registered, remove it and update the books.
    activationSlots.getOrElseUpdate(
      msg.activationId, {
        val timeoutHandler = actorSystem.scheduler.scheduleOnce(timeout) {
          processCompletion(msg.activationId, msg.transid, forced = true, isSystemError = false, invoker = instance)
        }

        // please note: timeoutHandler.cancel must be called on all non-timeout paths, e.g. Success
        ActivationEntry(
          msg.activationId,
          msg.user.namespace.uuid,
          instance,
          action.limits.memory.megabytes.MB,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          timeoutHandler,
          isBlackboxInvocation)
      })

    resultPromise
  }

  protected val messageProducer =
    messagingProvider.getProducer(config, Some(ActivationEntityLimit.MAX_ACTIVATION_LIMIT))

  /** 3. Send the activation to the invoker */
  protected def sendActivationToInvoker(producer: MessageProducer,
                                        msg: ActivationMessage,
                                        invoker: InvokerInstanceId): Future[RecordMetadata] = {
    implicit val transid: TransactionId = msg.transid

    val topic = s"invoker${invoker.toInt}"

    MetricEmitter.emitCounterMetric(LoggingMarkers.LOADBALANCER_ACTIVATION_START)
    val start = transid.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with activation id '${msg.activationId}'",
      logLevel = InfoLevel)

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transid.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = InfoLevel)
      case Failure(_) => transid.failed(this, start, s"error on posting to topic $topic")
    }

    // avs --begin
    /*val a_topic = s"load-invoker${invoker.toInt}"
    val a_msg: LoadMessage = LoadMessage(s"SanityCheck${invoker.toInt} and activID : ${msg.activationId}")
    producer.send(a_topic, a_msg).andThen {
      case Success(status) =>
        logging.info(this, s"<avs_debug> On topic: ${a_topic} SUCCESSFULLY posted message: ${a_msg} ")
      case Failure(_) => 
        logging.info(this, s"<avs_debug> On topic: ${a_topic} DIDNOT post message: ${a_msg} ")
    } */   
    // avs --end
  }

  /**
   * Subscribes to active acks (completion messages from the invokers), and
   * registers a handler for received active acks from invokers.
   */
  private val activationFeed: ActorRef =
    feedFactory.createFeed(actorSystem, messagingProvider, processAcknowledgement)

  /** 4. Get the active-ack message and parse it */
  protected[loadBalancer] def processAcknowledgement(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)
    AcknowledegmentMessage.parse(raw) match {
      case Success(m: CompletionMessage) =>
        processCompletion(
          m.activationId,
          m.transid,
          forced = false,
          isSystemError = m.isSystemError,
          invoker = m.invoker)
        activationFeed ! MessageFeed.Processed

      case Success(m: ResultMessage) =>
        processResult(m.response, m.transid)
        activationFeed ! MessageFeed.Processed

      case Failure(t) =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"failed processing message: $raw")

      case _ =>
        activationFeed ! MessageFeed.Processed
        logging.error(this, s"Unexpected Acknowledgment message received by loadbalancer: $raw")
    }
  }

// avs --begin

  type ntuiType = (ListBuffer[InvokerHealth]) => Boolean
  //def needToUpgradeInvoker(activeInvokers: ListBuffer[InvokerHealth]): Boolean = {
  val nTUI: ntuiType = (activeInvokers: ListBuffer[InvokerHealth]) => {
        var numUnsafeInvokers = 0 
        var retVal: Boolean = false

        activeInvokers.foreach{
          curInvoker =>
            var someWorkloadUnsafe: Boolean = false
            allInvokers.get(curInvoker.id) match {
              case Some(curInvokerStats) =>
                logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> invoker: ${curInvoker.id.toInt} is PRESENT in allInvokers. Before checking this invoker, numUnsafeInvokers: ${numUnsafeInvokers} ")
                someWorkloadUnsafe = curInvokerStats.checkInvokerStatus()
              case None =>
                allInvokers = allInvokers + (curInvoker.id -> new AdapativeInvokerStats(curInvoker.id,curInvoker.status,logging) )
                logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> invoker: ${curInvoker.id.toInt} is ABSENT in allInvokers. Before checking this invoker, numUnsafeInvokers: ${numUnsafeInvokers}. This shouldn't happen, HANDLE it!!")
                var tempInvokerStats = allInvokers(curInvoker.id)
                tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
                someWorkloadUnsafe = tempInvokerStats.checkInvokerStatus()
              }          
            if(someWorkloadUnsafe)
              numUnsafeInvokers+=1
        }

        if(activeInvokers.size > 0){
          if( (numUnsafeInvokers.toDouble/activeInvokers.size) > acceptableUnsafeInvokerRatio ){
            logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> <Res-1> numUnsafeInvokers: ${numUnsafeInvokers} activeInvokers.size: ${activeInvokers.size} and acceptableUnsafeInvokerRatio: ${acceptableUnsafeInvokerRatio}")
            retVal = true
          }else{
            logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> <Res-2> numUnsafeInvokers: ${numUnsafeInvokers} activeInvokers.size: ${activeInvokers.size} and acceptableUnsafeInvokerRatio: ${acceptableUnsafeInvokerRatio}")
          }
        }

      retVal 
    }

    def needToDowngradeInvoker(activeInvokers: ListBuffer[InvokerHealth]): ListBuffer[InvokerHealth] = {
      var toFillBuf: ListBuffer[InvokerHealth] = new mutable.ListBuffer[InvokerHealth]
      // getActiveNumConts
      activeInvokers.foreach{
        curInvoker =>
        allInvokers.get(curInvoker.id) match {
          case Some(curInvokerStats) =>
          logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> invoker: ${curInvoker.id.toInt} is PRESENT in allInvokers. Before checking this invoker, toFillBuf.size: ${toFillBuf.size} ")
          if(curInvokerStats.getActiveNumConts() == 0){ // don't have any active containers..
            toFillBuf+=curInvoker
          }
        case None =>
          allInvokers = allInvokers + (curInvoker.id -> new AdapativeInvokerStats(curInvoker.id,curInvoker.status,logging) )
          logging.info(this,s"<avs_debug> in <needToUpgradeInvoker> invoker: ${curInvoker.id.toInt} is ABSENT in allInvokers. Before checking this invoker, toFillBuf.size: ${toFillBuf.size}. This shouldn't happen, HANDLE it!!")
          var tempInvokerStats = allInvokers(curInvoker.id)
          tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
          if(tempInvokerStats.getActiveNumConts() == 0){ // don't have any active containers..
            toFillBuf+=curInvoker
          }
        }
      }
      toFillBuf
    }

  //type gifaType (String,InvokerHealth) => Option[InvokerInstanceId]
  type guifaType = (String) => Option[InvokerInstanceId]
  //def getUsedInvokerForAction(actionName: String,invoker: InvokerHealth): Option[InvokerInstanceId] = {
  val gUIFA: guifaType = (actionName: String) => {
    curRunningActions.get(actionName) match {
      case Some(curActStats) => 
        logging.info(this,s"<avs_debug> <getUsedInvokerForAction> action: ${actionName} is PRESENT in curRunningActions")
        curActStats.getUsedInvoker()
      case None => 
        logging.info(this,s"<avs_debug> <getUsedInvokerForAction> action: ${actionName} is ABSENT in curRunningActions")
        
        curRunningActions = curRunningActions + (actionName-> new ActionStats(actionName,logging))
        var myActStats :ActionStats = curRunningActions(actionName)
        myActStats.getUsedInvoker()
    }
  } 
  
  //def getActiveInvoker(actionName:String,curActiveInvoker:InvokerHealth,activeInvokers:IndexedSeq[InvokerHealth]): Option[InvokerInstanceId]
  type gaiType =  (String,ListBuffer[InvokerHealth]) => Option[InvokerInstanceId]
  val gaI: gaiType = (actionName:String,activeInvokers:ListBuffer[InvokerHealth]) => {
    curRunningActions.get(actionName) match {
      case Some(curActStats) => 
        logging.info(this,s"<avs_debug> <getActiveInvoker-0> action: ${actionName} is PRESENT in curRunningActions")
        curActStats.getActiveInvoker(activeInvokers)
      case None => 
        logging.info(this,s"<avs_debug> <getActiveInvoker-0> action: ${actionName} is ABSENT in curRunningActions")
        curRunningActions = curRunningActions + (actionName-> new ActionStats(actionName,logging))
        var myActStats :ActionStats = curRunningActions(actionName)
        myActStats.getActiveInvoker(activeInvokers)
    }
  }

  type aitype = (InvokerHealth,Int,Int) => Unit
  val aIT: aitype = (invoker: InvokerHealth,numCores:Int,memorySize:Int) => {
  //def addInvokerTracking(invoker: InvokerInstanceId,numCores:Int,memorySize:Int): Unit = {
    logging.info(this,s"<avs_debug> <addInvokerTracking> on invoker: ${invoker.id.toInt}")
    allInvokers.get(invoker.id) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<avs_debug> in <addInvokerTracking> invoker: ${invoker.id.toInt} is PRESENT in allInvokers ")
      case None =>
        allInvokers = allInvokers + (invoker.id -> new AdapativeInvokerStats(invoker.id,invoker.status,logging) )
        logging.info(this,s"<avs_debug> in <addInvokerTracking> invoker: ${invoker.id.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker.id)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
      } 
  }  
   
  def getInvokerTracking(invoker: InvokerInstanceId): AdapativeInvokerStats = {
    logging.info(this,s"<avs_debug> <getInvokerTracking> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<avs_debug> in <getInvokerTracking> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<avs_debug> in <getInvokerTracking> invoker: ${invoker.toInt} is ABSENT in allInvokers. Didn't expect this, HANDLE it!! ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats
    } 
  } 

  type cicType = (InvokerHealth,String) => Boolean
  val cIC: cicType = (invoker: InvokerHealth,actionName:String) => {
    //def checkInvokerCapacity(invoker: InvokerInstanceId): Boolean = {
    logging.info(this,s"<avs_debug> <checkInvokerCapacity> on invoker: ${invoker.id.toInt}")
    allInvokers.get(invoker.id) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<avs_debug> in <checkInvokerCapacity> invoker: ${invoker.id.toInt} is PRESENT in allInvokers ")
        curInvokerStats.capacityRemaining(actionName)
      case None =>
        allInvokers = allInvokers + (invoker.id -> new AdapativeInvokerStats(invoker.id,invoker.status,logging) )
        logging.info(this,s"<avs_debug> in <checkInvokerCapacity> invoker: ${invoker.id.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker.id)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.capacityRemaining(actionName)
      } 
  }

  def addActionStatsToInvoker(invoker: InvokerInstanceId,toUpdateAction:String,movingAvgLatency: Long, toUpdateNumConts:Int): Unit = {
    logging.info(this,s"<avs_debug> <addActionStatsToInvoker> on invoker: ${invoker.toInt}")
    allInvokers.get(invoker) match {
      case Some(curInvokerStats) =>
        logging.info(this,s"<avs_debug> in <addActionStatsToInvoker> invoker: ${invoker.toInt} is PRESENT in allInvokers ")
        curInvokerStats.updateActionStats(toUpdateAction,movingAvgLatency,toUpdateNumConts)
      case None =>
        allInvokers = allInvokers + (invoker -> new AdapativeInvokerStats(invoker,InvokerState.Healthy,logging) )
        logging.info(this,s"<avs_debug> in <addActionStatsToInvoker> invoker: ${invoker.toInt} is ABSENT in allInvokers ")
        var tempInvokerStats = allInvokers(invoker)
        tempInvokerStats.updateInvokerResource(4,8*1024) // defaulting to this..
        tempInvokerStats.updateActionStats(toUpdateAction,movingAvgLatency,toUpdateNumConts)
      } 
  } 

  private val loadRespFeed: ActorRef =
    loadFeedFactory.createFeed(actorSystem, messagingProvider, processLoadResponse)

  protected[loadBalancer] def processLoadResponse(bytes: Array[Byte]): Future[Unit] = Future{
    logging.info(this, s"<avs_debug> <processLoadResponse> 1")
    val raw = new String(bytes, StandardCharsets.UTF_8)

    ActionStatsMessage.parse(raw) match {
      
      case Success(m: ActionStatsMessage) =>
        logging.info(this,s"<avs_debug> <processLoadResponse> ActionStatsMessage successfully parsed! m.Str: ${m.toString}")
        var curInvokerStats = getInvokerTracking(m.invoker) // assuming that this'd never fail! 
        curRunningActions.get(m.curActName) match {
          case Some(curActStats) => 
            logging.info(this,s"<avs_debug> <processLoadResponse> action: ${m.curActName} is PRESENT in curRunningActions and it ran on invoker: ${m.invoker.toInt}")
            //curActStats.addActionStats(m.invoker,m.avgLatency,m.numConts) // curActStats.simplePrint(m.curActName,m.avgLatency,m.numConts,logging)
            curActStats.addActionStats(m.invoker,curInvokerStats,m.avgLatency,m.numConts)
          case None => 
            logging.info(this,s"<avs_debug> <processLoadResponse> action: ${m.curActName} is ABSENT in curRunningActions  and it ran on invoker: ${m.invoker.toInt}")
            
            curRunningActions = curRunningActions + (m.curActName -> new ActionStats(m.curActName,logging))
            //curRunningActions = curRunningActions + (m.curActName -> curInvokerStats)
            var myActStats :ActionStats = curRunningActions(m.curActName)
            myActStats.addActionStats(m.invoker,curInvokerStats,m.avgLatency,m.numConts) //myActStats.simplePrint(m.curActName,m.avgLatency,m.numConts,logging)
        }

        //addActionStatsToInvoker(m.invoker,m.curActName,m.avgLatency,m.numConts)
      case Failure(t) =>
        logging.info(this,s"<avs_debug> <processLoadResponse> ActionStatsMessage wasn't parsed! raw: ${raw}")
    }    
    //logging.info(this, s"<avs_debug> <processLoadResponse> raw_message: ${raw} actStatsMsg --> {msg.toString} loadMsg --> {msg2.toString}")
    loadRespFeed ! MessageFeed.Processed
  }  
// avs --end

  /** 5. Process the result ack and return it to the user */
  protected def processResult(response: Either[ActivationId, WhiskActivation], tid: TransactionId): Unit = {
    val aid = response.fold(l => l, r => r.activationId)

    // Resolve the promise to send the result back to the user.
    // The activation will be removed from the activation slots later, when the completion message
    // is received (because the slot in the invoker is not yet free for new activations).
    activationPromises.remove(aid).foreach(_.trySuccess(response))
    logging.info(this, s"received result ack for '$aid'")(tid)
  }

  protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry)

  /** 6. Process the completion ack and update the state */
  protected[loadBalancer] def processCompletion(aid: ActivationId,
                                                tid: TransactionId,
                                                forced: Boolean,
                                                isSystemError: Boolean,
                                                invoker: InvokerInstanceId): Unit = {

    val invocationResult = if (forced) {
      InvocationFinishedResult.Timeout
    } else {
      // If the response contains a system error, report that, otherwise report Success
      // Left generally is considered a Success, since that could be a message not fitting into Kafka
      if (isSystemError) {
        InvocationFinishedResult.SystemError
      } else {
        InvocationFinishedResult.Success
      }
    }

    activationSlots.remove(aid) match {
      case Some(entry) =>
        totalActivations.decrement()
        val totalActivationMemory =
          if (entry.isBlackbox) totalBlackBoxActivationMemory else totalManagedActivationMemory
        totalActivationMemory.add(entry.memory.toMB * (-1))
        activationsPerNamespace.get(entry.namespaceId).foreach(_.decrement())

        releaseInvoker(invoker, entry)

        if (!forced) {
          entry.timeoutHandler.cancel()
          // notice here that the activationPromises is not touched, because the expectation is that
          // the active ack is received as expected, and processing that message removed the promise
          // from the corresponding map
        } else {
          // the entry has timed out; if the active ack is still around, remove its entry also
          // and complete the promise with a failure if necessary
          activationPromises
            .remove(aid)
            .foreach(_.tryFailure(new Throwable("no completion or active ack received yet")))
        }

        logging.info(this, s"${if (!forced) "received" else "forced"} completion ack for '$aid'")(tid)
        // Active acks that are received here are strictly from user actions - health actions are not part of
        // the load balancer's activation map. Inform the invoker pool supervisor of the user action completion.
        // guard this
        invokerPool ! InvocationFinishedMessage(invoker, invocationResult)
      case None if tid == TransactionId.invokerHealth =>
        // Health actions do not have an ActivationEntry as they are written on the message bus directly. Their result
        // is important to pass to the invokerPool because they are used to determine if the invoker can be considered
        // healthy again.
        logging.info(this, s"received completion ack for health action on $invoker")(tid)
        // guard this
        invokerPool ! InvocationFinishedMessage(invoker, invocationResult)
      case None if !forced =>
        // Received an active-ack that has already been taken out of the state because of a timeout (forced active-ack).
        // The result is ignored because a timeout has already been reported to the invokerPool per the force.
        logging.debug(this, s"received completion ack for '$aid' which has no entry")(tid)
      case None =>
        // The entry has already been removed by an active ack. This part of the code is reached by the timeout and can
        // happen if active-ack and timeout happen roughly at the same time (the timeout was triggered before the active
        // ack canceled the timer). As the active ack is already processed we don't have to do anything here.
        logging.debug(this, s"forced completion ack for '$aid' which has no entry")(tid)
    }
  }
}
