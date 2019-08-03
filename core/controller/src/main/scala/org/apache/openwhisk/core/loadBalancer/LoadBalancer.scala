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

import scala.concurrent.Future
import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.spi.Spi
import scala.concurrent.duration._
import scala.collection.mutable //avs
import scala.collection.immutable //avs
//import scala.collection.mutable.ListBuffer //avs

// avs --begin

class functionInfo {
  // avs --begin
  var containerStandaloneRuntime = immutable.Map.empty[String,Double] 
  containerStandaloneRuntime = containerStandaloneRuntime + ("imageResizing_v1"->635.0)
  containerStandaloneRuntime = containerStandaloneRuntime + ("rodinia_nn_v1"->6350.0)
  containerStandaloneRuntime = containerStandaloneRuntime + ("euler3d_cpu_v1"->18000.0)
  containerStandaloneRuntime = containerStandaloneRuntime + ("servingCNN_v1"->1800.0)
  containerStandaloneRuntime = containerStandaloneRuntime + ("realTimeAnalytics_v1"->550.0)
  containerStandaloneRuntime = containerStandaloneRuntime + ("invokerHealthTestAction0"->0.0)
  
  def addFunctionRuntime(functionName: String): Unit = {
    if(functionName == "imageResizing_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 635.0)  
    }else if (functionName == "rodinia_nn_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 6350.0)  
    }else if (functionName == "euler3d_cpu_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 18000.0)  
    }else if (functionName == "servingCNN_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 1800.0)  
    }else if (functionName =="realTimeAnalytics_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 550.0)  
    }
    else if (functionName == "invokerHealthTestAction0"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 1350.0)  
    }
  }   

  def getFunctionRuntime(functionName: String): Double = {
    containerStandaloneRuntime.get(functionName) match {
      case Some(funcStandaloneRuntime) => 
      funcStandaloneRuntime
    case None =>
      var maxRuntime:Double = 60*5*1000.0
      maxRuntime
    }
  }

  def getActionType(functionName: String): String = {
    if(functionName == "imageResizing_v1"){
        "ET" 
    }else if (functionName == "rodinia_nn_v1"){
        "MP" 
    }else if (functionName == "euler3d_cpu_v1"){
        "MP"
    }else if (functionName == "servingCNN_v1"){
        "ET" 
    }else if (functionName == "realTimeAnalytics_v1"){
        "ET"       
    }else{
        "MP"
    }
  }
// avs --end  
}

// the stats of an action in a given invoker
class ActionStatsPerInvoker(val actionName: String,logging: Logging){
  var numConts = 0
  var movingAvgLatency: Long = 0 
  var actionType: String  = "MP" // ET or MessagingProvider
  var standaloneRuntime = 0
  var opZone = 0 // 0: safe ( 0 to 50% of latency); 1: will reach un-safe soon, 2: unsafe
  var lastUpdated = 0 // TODO: should be time, will update TYPE later.

  def simplePrint(toPrintAction:String, toPrintLatency: Long, toPrintNumConts:Int): Unit = {
   logging.info(this,s"\t <avs_debug> <simplePrint> Action: ${toPrintAction} has averageLatency: ${toPrintLatency} and #conts: ${toPrintNumConts}") 
  }
}

// the stats of an action across all invokers. Tracked per Invoker.
class ActionStats(val actionName:String,logging: Logging){
  var usedInvokers = mutable.Map.empty[InvokerInstanceId, ActionStatsPerInvoker]

  def addActionStats(invoker: InvokerInstanceId,movingAvgLatency: Long, toUpdateNumConts: Int){
    usedInvokers.get(invoker) match{
      case Some(curInvokerActStats) =>
        logging.info(this,s"\t <avs_debug> <ActionStats> <addActionStats> Action: ${actionName}, invoker: ${invoker.toInt} is PRESENT. NumConts: ${toUpdateNumConts} and avgLat: ${movingAvgLatency}")
        curInvokerActStats.numConts = toUpdateNumConts
        curInvokerActStats.movingAvgLatency = movingAvgLatency

      case None =>
        // allActions = allActions + (toAddAction -> new ActionStatsPerInvoker(toAddAction))
        // allInvokers = allInvokers + (invoker.id -> new AdapativeInvokerStats(invoker.id,invoker.status) )
        usedInvokers = usedInvokers + (invoker -> new ActionStatsPerInvoker(actionName,logging))
        var tempInvokerActStats: ActionStatsPerInvoker = usedInvokers(invoker)
        logging.info(this,s"\t <avs_debug> <ActionStats> <addActionStats> Action: ${actionName}, invoker: ${invoker.toInt} is ABSENT, adding it to usedInvokers. NumConts: ${toUpdateNumConts} and avgLat: ${movingAvgLatency}")
        tempInvokerActStats.numConts = toUpdateNumConts
        tempInvokerActStats.movingAvgLatency = movingAvgLatency

    }
  }

}

class InvokerResources(var numCores: Int, var memorySize: Int){

}

class AdapativeInvokerStats(val id: InvokerInstanceId, val status: InvokerState,logging: Logging) extends functionInfo{
  // begin - copied from InvokerHealth
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: AdapativeInvokerStats => that.id == this.id && that.status == this.status
    case _                   => false
  }

  override def toString = s"AdapativeInvokerStats($id, $status)"
  // end - copied from InvokerHealth
  var myResources = new InvokerResources(0,0) 
  var numETConts = 0
  var numMPConts = 0
  var allActions = mutable.Map.empty[String, ActionStatsPerInvoker]

  def updateInvokerResource(toSetNumCores:Int,toSetMemory: Int): Unit = {
    myResources.numCores = toSetNumCores
    myResources.memorySize = toSetMemory
  }

  def addAction(toAddAction: String): Unit = {
    logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <addAction> Trying to add action: ${toAddAction} to allActions")
    allActions.get(toAddAction) match {
      case Some (curAction) =>
        logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <addAction> invoker: ${id.toInt} Ok action ${toAddAction} IS present in allActions, doing nothing!")
      case None => 
        logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <addAction> invoker: ${id.toInt} Ok action ${toAddAction} is NOT present in allActions, adding it..")
        allActions = allActions + (toAddAction -> new ActionStatsPerInvoker(toAddAction,logging))
    }
    var myActType = getActionType(toAddAction);
    var myStandaloneRuntime = getFunctionRuntime(toAddAction)
    logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <addAction> Action: ${toAddAction} is of type: ${myActType} and runtime: ${myStandaloneRuntime}")
  }

  def updateActionStats(toUpdateAction:String, movingAvgLatency: Long, toUpdateNumConts:Int){
    allActions.get(toUpdateAction) match {
      case Some(curActStats) => 
        curActStats.numConts = toUpdateNumConts
        curActStats.movingAvgLatency = movingAvgLatency
        logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <updateActionStats> 1. invoker: ${id.toInt} action: ${toUpdateAction} numConts: ${curActStats.numConts} movingAvgLatency: ${curActStats.movingAvgLatency}")     

      case None =>
        allActions = allActions + (toUpdateAction -> new ActionStatsPerInvoker(toUpdateAction,logging))
        var tempActStats: ActionStatsPerInvoker = allActions(toUpdateAction)
        tempActStats.numConts = toUpdateNumConts
        tempActStats.movingAvgLatency = movingAvgLatency
        logging.info(this,s"\t <avs_debug> <AdapativeInvokerStats> <updateActionStats> 2. invoker: ${id.toInt} action: ${toUpdateAction} numConts: ${tempActStats.numConts} movingAvgLatency: ${tempActStats.movingAvgLatency}")     
    }
  }

  def capacityRemaining(): Boolean = {
    // should update based on -- memory; #et, #mp
    true
  }

}
// avs --end

/**
 * Describes an abstract invoker. An invoker is a local container pool manager that
 * is in charge of the container life cycle management.
 *
 * @param id a unique instance identifier for the invoker
 * @param status it status (healthy, unhealthy, offline)
 */
class InvokerHealth(val id: InvokerInstanceId, val status: InvokerState) {
  //var myStats: AdapativeInvokerStats = new AdapativeInvokerStats(id,status) // avs
  override def equals(obj: scala.Any): Boolean = obj match {
    case that: InvokerHealth => that.id == this.id && that.status == this.status
    case _                   => false
  }

  override def toString = s"InvokerHealth($id, $status)"
}

trait LoadBalancer {

  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action the action to invoke
   * @param msg the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]]

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
   *
   * @return a Future[IndexedSeq[InvokerHealth]] representing the health of the pools managed by the loadbalancer.
   */
  def invokerHealth(): Future[IndexedSeq[InvokerHealth]]

  /** Gets the number of in-flight activations for a specific user. */
  def activeActivationsFor(namespace: UUID): Future[Int]

  /** Gets the number of in-flight activations in the system. */
  def totalActiveActivations: Future[Int]

  /** Gets the size of the cluster all loadbalancers are acting in */
  def clusterSize: Int = 1
}

/**
 * An Spi for providing load balancer implementations.
 */
trait LoadBalancerProvider extends Spi {
  def requiredProperties: Map[String, String]

  def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                         logging: Logging,
                                                                         materializer: ActorMaterializer): LoadBalancer

  /** Return default FeedFactory */
  def createFeedFactory(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): FeedFactory = {

    val activeAckTopic = s"completed${instance.asString}"
    val maxActiveAcksPerPoll = 128
    val activeAckPollDuration = 1.second

    new FeedFactory {
      def createFeed(f: ActorRefFactory, provider: MessagingProvider, acker: Array[Byte] => Future[Unit]) = {
        f.actorOf(Props {
          new MessageFeed(
            "activeack",
            logging,
            provider.getConsumer(whiskConfig, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll),
            maxActiveAcksPerPoll,
            activeAckPollDuration,
            acker)
        })
      }
    }
  }
// avs --begin
  def createLoadFeedFactory(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): FeedFactory = {

    val activeAckTopic = s"load-completed${instance.asString}"
    val maxActiveAcksPerPoll = 128
    val activeAckPollDuration = 1.second

    new FeedFactory {
      def createFeed(f: ActorRefFactory, provider: MessagingProvider, acker: Array[Byte] => Future[Unit]) = {
        f.actorOf(Props {
          new MessageFeed(
            "loadResponse",
            logging,
            provider.getConsumer(whiskConfig, activeAckTopic, activeAckTopic, maxPeek = maxActiveAcksPerPoll),
            maxActiveAcksPerPoll,
            activeAckPollDuration,
            acker)
        })
      }
    }
  }
// avs --end  
}

/** Exception thrown by the loadbalancer */
case class LoadBalancerException(msg: String) extends Throwable(msg)
