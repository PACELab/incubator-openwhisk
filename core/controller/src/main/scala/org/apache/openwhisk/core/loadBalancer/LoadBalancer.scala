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
import scala.collection.mutable.ListBuffer //avs
import java.time.Instant // avs
import scala.collection.immutable.ListMap // avs
//import util.control.Breaks._ // avs
// avs --begin

class functionInfo {
  // avs --begin
  var containerStandaloneRuntime = immutable.Map.empty[String,Long] 
  containerStandaloneRuntime = containerStandaloneRuntime + ("imageResizing_v1"->635)
  containerStandaloneRuntime = containerStandaloneRuntime + ("rodinia_nn_v1"->6350)
  containerStandaloneRuntime = containerStandaloneRuntime + ("euler3d_cpu_v1"->18000)
  containerStandaloneRuntime = containerStandaloneRuntime + ("servingCNN_v1"->1800)
  containerStandaloneRuntime = containerStandaloneRuntime + ("realTimeAnalytics_v1"->550)
  containerStandaloneRuntime = containerStandaloneRuntime + ("invokerHealthTestAction0"->0)
  
  def addFunctionRuntime(functionName: String): Unit = {
    if(functionName == "imageResizing_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 635)  
    }else if (functionName == "rodinia_nn_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 6350)  
    }else if (functionName == "euler3d_cpu_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 18000)  
    }else if (functionName == "servingCNN_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 1800)  
    }else if (functionName =="realTimeAnalytics_v1"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 550)  
    }
    else if (functionName == "invokerHealthTestAction0"){
      containerStandaloneRuntime = containerStandaloneRuntime + (functionName -> 1350)  
    }
  }   

  def getFunctionRuntime(functionName: String): Long = {
    containerStandaloneRuntime.get(functionName) match {
      case Some(funcStandaloneRuntime) => 
      funcStandaloneRuntime
    case None =>
      var maxRuntime:Long = 60*5*1000
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

  var latencyTolerance = 1.15
  var safeBeginThreshold:Double = 0.0
  var safeEndThreshold:Double = 0.5
  var warnBeginThreshold:Double = safeEndThreshold
  var warnEndThreshold:Double = 0.75
  var unsafeBeginThreshold:Double = warnEndThreshold
  var unsafeEndThreshold:Double = 100000.0 

  var opZoneSafe = 0
  var opZoneWarn = 1
  var opZoneUnSafe = 2

  var statsTimeoutInMilli: Long = 60*1000 // 1 minute is the time for docker to die. So, the stats are going to be outdate.
  var heartbeatTimeoutInMilli: Long = 20*1000
  var resetNumInstances = 2 
  var minResetTimeInMilli = 3*1000
  var movWindow_numReqs = 10
// avs --end  
}

// the stats of an action in a given invoker
class ActionStatsPerInvoker(val actionName: String,val myInvokerID: Int,logging: Logging) extends functionInfo{
  val standaloneRuntime: Long = getFunctionRuntime(actionName)
  val statsResetTimeout: Long = if( (resetNumInstances * standaloneRuntime) > minResetTimeInMilli) resetNumInstances * standaloneRuntime else minResetTimeInMilli
  var numConts = 0
  var movingAvgLatency: Long = 0 
  var cumulSum: Long = 0
  var runningCount: Long = 0
  var actionType: String  = "MP" // ET or MessagingProvider
  var opZone = 0 // 0: safe ( 0 to 50% of latency); 1: will reach un-safe soon, 2: unsafe
  var lastUpdated: Long = Instant.now.toEpochMilli
  // FIX-IT: should be more accurate (based on when responses come?) instead of estimates..
  var proactiveTimeoutInMilli: Long =  statsTimeoutInMilli + (10*1000) + (1*standaloneRuntime) 
  
  def simplePrint(toPrintAction:String, toPrintLatency: Long, toPrintNumConts:Int): Unit = {
   logging.info(this,s"\t <avs_debug> <simplePrint> <ASPI> Action: ${toPrintAction} has averageLatency: ${toPrintLatency} and #conts: ${toPrintNumConts}") 
  }

  def opZoneUpdate(): Unit = {
    var toSetOpZone = opZone
    var latencyRatio: Double = (movingAvgLatency.toDouble/standaloneRuntime)
    var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold
    logging.info(this,s"\t <avs_debug> <ASPI:opZoneUpdate> action: ${actionName} and curOpZone is ${opZone} movingAvgLatency: ${movingAvgLatency} and standaloneRuntime: ${standaloneRuntime} and latencyRatio: ${latencyRatio} and toleranceRatio: ${toleranceRatio}") 

    if( (toleranceRatio >= safeBeginThreshold) && (toleranceRatio <= safeEndThreshold)){
        opZone = opZoneSafe
        logging.info(this,s"\t <avs_debug> <ASPI:opZoneUpdate> action:${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${safeBeginThreshold} and end: ${safeEndThreshold}, so opzone is SAFE --${opZone}.") 
    }else if( (toleranceRatio >= warnBeginThreshold) && (toleranceRatio <= warnEndThreshold)){
      opZone = opZoneWarn
      logging.info(this,s"\t <avs_debug> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${warnBeginThreshold} and end: ${warnEndThreshold}, so opzone is in WARNING safe --${opZone}") 
    }else if( (toleranceRatio >= unsafeBeginThreshold) && (toleranceRatio <= unsafeEndThreshold)){
      opZone = opZoneUnSafe
      logging.info(this,s"\t <avs_debug> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${unsafeBeginThreshold} and end: ${unsafeEndThreshold}, so opzone is UNSAFE --${opZone}") 
    }else{
      opZone = opZoneUnSafe
      logging.info(this,s"\t <avs_debug> <ASPI:opZoneUpdate> action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently in a weird region, so it should be declared UNSAFE --${opZone}") 
    }
  }

  def resetStats(curTime: Long,statsResetFlag: Boolean): Unit = {
    // Assuming that, if either it is not updated in the past, 
    // a. statsTimeoutInMilli : Atleast a container would have died.
    // b. statsResetTimeout: It would have passed some time, so, the load should have subsided..

    // EXPT-WARNING: Might be better to issue a load request and refresh stats!, instead of resetting willy nilly!
    if(statsResetFlag && opZone != opZoneUnSafe){
      logging.info(this,s"\t <avs_debug> <ASPI:resetStats> action: ${actionName}, myInvokerID: ${myInvokerID} has passed statsResetFlag: ${statsResetFlag} but opZone is ${opZone} not unsafe, not doing anything..")       
    }else{      
      val prevOpZone = opZone
      if(statsResetFlag && opZone == opZoneUnSafe){
        numConts = 1
      }else{
        numConts = 0
      }

      opZone = 0 
      movingAvgLatency = 0
      cumulSum = 0
      runningCount = 0
    
      lastUpdated = Instant.now.toEpochMilli        
      logging.info(this,s"\t <avs_debug> <ASPI:resetStats> action: ${actionName}, myInvokerID: ${myInvokerID} resetting my stats at lastUpdated: ${lastUpdated} and statsResetFlag: ${statsResetFlag} and prevOpZone: ${prevOpZone}") 
      
    }
    
  }

  // update(latencyVal,initTime,toUpdateNumConts)
  def update(latency: Long, initTime: Long, toUpdateNumConts: Int): Unit = {
    if(initTime==0){ // warm starts only!!
      cumulSum+= latency  
      runningCount+= 1
      lastUpdated = Instant.now.toEpochMilli      
      movingAvgLatency = if(runningCount>0) (cumulSum/runningCount) else 0
      if(runningCount>movWindow_numReqs) 
        opZoneUpdate()      
    }
    numConts = toUpdateNumConts
    logging.info(this,s"\t <avs_debug> <ASPI:update> In update of Action: ${actionName} myInvokerID: ${myInvokerID}, latency: ${latency} count: ${runningCount} cumulSum: ${cumulSum} movingAvgLatency: ${movingAvgLatency} numConts: ${numConts} opZone: ${opZone} ")     
  }

}

class InvokerRunningState(var numInFlightReqs: Int,var lastUsed: Long,var invokerRank:Int){

}
// the stats of an action across all invokers. Tracked per Invoker.
class ActionStats(val actionName:String,logging: Logging){
  var usedInvokers = mutable.Map.empty[InvokerInstanceId, AdapativeInvokerStats]
  var lastInstantUsed = mutable.Map.empty[InvokerInstanceId, Long]
  var cmplxLastInstUsed = mutable.Map.empty[InvokerInstanceId, InvokerRunningState]

  def addActionStats(invoker: InvokerInstanceId,invokerStats:AdapativeInvokerStats,latencyVal: Long,initTime: Long, toUpdateNumConts: Int){
    
    usedInvokers.get(invoker) match{
      case Some(curInvokerStats) =>
        logging.info(this,s"\t <avs_debug> <ActionStats> <addActionStats> Action: ${actionName}, invoker: ${invoker.toInt} is PRESENT. NumConts: ${toUpdateNumConts} and avgLat: ${latencyVal} initTime: ${initTime}")
        // updateActionStats(toUpdateAction:String, latencyVal: Long, toUpdateNumConts:Int):Unit = {
        curInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts)
        //lastInstantUsed(invoker) = curInstant // should be ok, but will only update when it is allocated..
      case None =>
        usedInvokers = usedInvokers + (invoker -> invokerStats)

        var curInstant: Long = Instant.now.toEpochMilli
        lastInstantUsed = lastInstantUsed + (invoker -> curInstant)
        cmplxLastInstUsed = cmplxLastInstUsed + (invoker -> new InvokerRunningState(0,curInstant,invoker.toInt))
        var tempInvokerStats:AdapativeInvokerStats  = usedInvokers(invoker)

        logging.info(this,s"\t <avs_debug> <ActionStats> <addActionStats> Action: ${actionName}, invoker: ${invoker.toInt} is ABSENT, adding it to usedInvokers. NumConts: ${toUpdateNumConts} and avgLat: ${latencyVal} at instant: ${curInstant} initTime: ${initTime}")
        tempInvokerStats.updateActionStats(actionName,latencyVal,initTime,toUpdateNumConts)
    }
  } 

  def getAutoScaleUsedInvoker(): Option[InvokerInstanceId] = {
    //var rankOrderedInvokers = cmplxLastInstUsed.toSeq.sortBy(curEle => (curEle._2.invokerRank)) //(Ordering[(Int,Long)].reverse)
    //var rankOrderedInvokers = ListMap(cmplxLastInstUsed.toSeq.sortWith(_._2.invokerRank < _._2.invokerRank):_*) // OG-AUTOSCALE!
    var rankOrderedInvokers = ListMap(cmplxLastInstUsed.toSeq.sortWith(_._2.invokerRank > _._2.invokerRank):_*)

    rankOrderedInvokers.keys.foreach{
      curInvoker =>
      var curInvokerRunningState = cmplxLastInstUsed(curInvoker)
      usedInvokers.get(curInvoker) match {
        case Some(curInvokerStats) => 
          val curInvokerRunningState: InvokerRunningState = cmplxLastInstUsed(curInvoker)
          logging.info(this,s"\t <avs_debug> <gasUI> Action: ${actionName}, invoker: ${curInvokerRunningState.invokerRank} was the invoker used at ${curInvokerRunningState.lastUsed} and had #inflight-reqs: ${curInvokerRunningState.numInFlightReqs} ")
          logging.info(this,s"\t <avs_debug> <gasUI> Action: ${actionName}, invoker: ${curInvoker.toInt} checking whether it has any capacityRemaining...")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          //var curInvokerStats: AdapativeInvokerStats = usedInvokers(curInvoker)
          val (numInFlightReqs,decision) = curInvokerStats.capacityRemaining(actionName)
          if(decision){
            var curInstant: Long = Instant.now.toEpochMilli
            lastInstantUsed(curInvoker) = curInstant

            cmplxLastInstUsed(curInvoker).numInFlightReqs = numInFlightReqs
            cmplxLastInstUsed(curInvoker).lastUsed = curInstant

            logging.info(this,s"\t <avs_debug> <gasUI> Invoker: ${curInvoker.toInt} supposedly has capacity, am I yielding at instant: ${curInstant}, lastInstantUsed(curInvoker): ${lastInstantUsed(curInvoker)}")  
            return Some(curInvoker)
          } 
        case None =>
          logging.info(this,s"\t <avs_debug> <getUsedInvoker> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")

      }
    }
    logging.info(this,s"\t <avs_debug> <gasUI> Action: ${actionName} did not get a used invoker :( :( ")
    None
  }
  // return type: (nextInvoker,nextInvokerNumReqs,curInvokerNumDummyReqs,nextInvokerProactiveDecision)
  def isAutoscaleProactiveNeeded(toCheckInvoker: InvokerInstanceId,myProactiveMaxReqs: Int,nextInvokerMaxProactiveReqs: Int): (InvokerInstanceId,Int,Int,Boolean) = {

    //var rankOrderedInvokers = cmplxLastInstUsed.toSeq.sortBy(curEle => (curEle._2.invokerRank)) //(Ordering[(Int,Long)].reverse)
    val tempRankOrderedInvokers = ListMap(cmplxLastInstUsed.toSeq.sortWith(_._2.invokerRank > _._2.invokerRank):_*)
    val rankSortedInvokers = tempRankOrderedInvokers.keys.toList
    var mainLoopIdx = toCheckInvoker.toInt // assuming it starts from 0-index

    usedInvokers.get(toCheckInvoker) match {
      case Some(curInvokerStats) => 
        logging.info(this,s"\t <avs_debug> <IASPN> Action: ${actionName}, toCheckInvoker: ${toCheckInvoker.toInt} checking whether it requires proactive Spawning..")
          // If I fit, I will choose this.
        val (curInvokerNumContsToSpawn,decision) = curInvokerStats.checkInvokerActTypeOpZone(actionName,myProactiveMaxReqs)
          
        if(decision){ 
          //var internalLoopIdx = -1 // // OG-AUTOSCALE!
          var internalLoopIdx = rankSortedInvokers.size
          rankSortedInvokers.foreach{
            nextInvoker =>
            internalLoopIdx-=1
            //internalLoopIdx+=1  // OG-AUTOSCALE!
            //if(internalLoopIdx>mainLoopIdx){ // so all the invokers before me aren't used already for a reason--they are all busy, so will choose the next one.."  // OG-AUTOSCALE!
            if(internalLoopIdx < mainLoopIdx){ // so all the invokers before me aren't used already for a reason--they are all busy, so will choose the next one.."
              logging.info(this,s"<avs_debug> <IASPN> 0.0 toCheckInvoker: ${toCheckInvoker.toInt} internalLoopIdx: ${internalLoopIdx} checking whether I can issue dummy req to nextInvoker: ${nextInvoker.toInt} ")
              usedInvokers.get(nextInvoker) match {
                case Some(nextInvokerStats) =>
                  //val (actualNumDummyReqs,canIssueDummyReqs) = canIssueDummyReqToInvoker(proactiveInvoker,action.name.asString,schedulingState.numProactiveContsToSpawn)
                  // canDummyReqBeIssued(actionName: String,numDummyReqs: Int)
                  val (nextInvokerNumDummyReqsToIssue,nextInvokerDummyReqDecision) = nextInvokerStats.canDummyReqBeIssued(actionName,nextInvokerMaxProactiveReqs)
                  if(nextInvokerDummyReqDecision){
                    logging.info(this,s"\t <avs_debug> <IASPN> 1.0 toCheckInvoker: ${toCheckInvoker.toInt} nextInvoker: ${nextInvoker.toInt} can accommodate ${nextInvokerNumDummyReqsToIssue} dummy reqs")  
                    return (nextInvoker,nextInvokerNumDummyReqsToIssue,curInvokerNumContsToSpawn,decision)
                  }else{
                    logging.info(this,s"\t <avs_debug> <IASPN> 1.5 toCheckInvoker: ${toCheckInvoker.toInt} nextInvoker: ${nextInvoker.toInt} WON'T or CAN'T accommodate ${nextInvokerNumDummyReqsToIssue} dummy reqs")  
                  }
                case None =>
                  logging.info(this,s"\t <avs_debug> <IASPN> 2.0 toCheckInvoker: ${toCheckInvoker.toInt} supposedly needs proactive cont but couldn't locate nextInvoker: ${nextInvoker.toInt}'s stats' ")
                  return (toCheckInvoker,0,0,false)    
              }
            }else
              logging.info(this,s"\t <avs_debug> <IASPN> 0.5 curIter: ${internalLoopIdx}'s invoker: ${nextInvoker.toInt} earlier than toCheckInvoker: ${toCheckInvoker.toInt} ")

          }
          // can just return true for myself and atleast issue proactive invokers in myself..
          logging.info(this,s"\t <avs_debug> <IASPN> 2.5 toCheckInvoker: ${toCheckInvoker.toInt} supposedly needs proactive cont but couldn't locate nextInvoker stats'. Will issues ${curInvokerNumContsToSpawn} dummy reqs to myself! ")
          return (toCheckInvoker,0,curInvokerNumContsToSpawn,decision)
        }
        else{
          logging.info(this,s"\t <avs_debug> <IASPN> 3.0 Invoker: ${toCheckInvoker.toInt} supposedly does NOT and size of rankSortedInvokers: ${rankSortedInvokers.size}")  
          // anyway won't spawn an invoker..
          return (toCheckInvoker,0,0,false)
        }
      case None =>
          logging.info(this,s"\t <avs_debug> <IASPN> 4.0 Invoker: ${toCheckInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")

    }
    // anyway won't spawn an invoker..
    (toCheckInvoker,0,0,false)
  }

  def getActiveInvoker(activeInvokers: ListBuffer[InvokerHealth]): Option[InvokerInstanceId] = {
    activeInvokers.foreach{
      curInvoker => 
      usedInvokers.get(curInvoker.id) match {
        case Some(curInvokerStats) => 
          logging.info(this,s"\t <avs_debug> <getActiveInvoker> Action: ${actionName}, invoker: ${curInvoker.id.toInt} checking whether it has any capacityRemaining...")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          val (numInFlightReqs,decision) = curInvokerStats.capacityRemaining(actionName)
          if(decision){

            var curInstant: Long = Instant.now.toEpochMilli
            lastInstantUsed(curInvoker.id) = curInstant

            cmplxLastInstUsed(curInvoker.id).numInFlightReqs = numInFlightReqs
            cmplxLastInstUsed(curInvoker.id).lastUsed = curInstant            

            logging.info(this,s"\t <avs_debug> <getActiveInvoker> Invoker: ${curInvoker.id.toInt} supposedly has capacity, am I yielding at instant: ${curInstant}, lastInstantUsed(curInvoker): ${lastInstantUsed(curInvoker.id)}")  

            return Some(curInvoker.id)
          }
        case None =>
          logging.info(this,s"\t <avs_debug> <getActiveInvoker> Invoker: ${curInvoker.id.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
      }
    }
    // if it has come here, then I don't have anything..     
    logging.info(this,s"\t <avs_debug> <getActiveInvoker> Action: ${actionName} did not get an active invoker :( :(, which is of size: ${activeInvokers.size}")
    None
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
  var myResources = new InvokerResources(4,8*1024) // for now this is by default assumed, should make it parameterized.
  var numConts = mutable.Map.empty[String,Int] // actionType, numContsOf this action-type
  numConts = numConts + ("ET" -> 0)
  numConts = numConts + ("MP" -> 0)
  
  // Max of any action of myType.
  var actionTypeOpZone = mutable.Map.empty[String,Int] // actionType, numContsOf this action-type
  actionTypeOpZone = actionTypeOpZone + ("ET" -> opZoneSafe)
  actionTypeOpZone = actionTypeOpZone + ("MP" -> opZoneSafe)

  var allActions = mutable.Map.empty[String, ActionStatsPerInvoker]
  //var allActionsByType = mutable.Map.empty[String, ListBuffer[String]]
  //allActionsByType = allActionsByType + ("ET" -> new mutable.ListBuffer[String])
  //allActionsByType = allActionsByType + ("MP" -> new mutable.ListBuffer[String])

  var allActionsByType = mutable.Map.empty[String, mutable.Map[String,Int]]
  allActionsByType = allActionsByType + ("ET" -> mutable.Map.empty[String,Int])
  allActionsByType = allActionsByType + ("MP" -> mutable.Map.empty[String,Int])

  var inFlightReqsByType = mutable.Map.empty[String, Int]
  inFlightReqsByType = inFlightReqsByType + ("ET" -> 0)
  inFlightReqsByType = inFlightReqsByType + ("MP" -> 0)

  var statsUpdatedByAction = mutable.Map.empty[String,Long] // timestamp of last action..

  // Instant.now.toEpochMilli

  var maxInFlightReqsByType = mutable.Map.empty[String, Double]
  maxInFlightReqsByType = maxInFlightReqsByType + ("ET" -> 1.5 * myResources.numCores)
  maxInFlightReqsByType = maxInFlightReqsByType + ("MP" -> 1.0 * myResources.numCores)

  // -------------- Thresholds --------------
  //var maxInFlightReqs_ET = 1.5 * myResources.numCores
  //var maxInFlightReqs_MP = 1.0 * myResources.numCores
  var warningZoneThd:Double = 0.5
  var maxProactiveNumConts: Int = 2
  var curNumProcactiveConts: Int = 0
  var lastTime_ProactivelySpawned: Long = 0 //Instant.now.toEpochMilli
  // -------------- Thresholds --------------

  def updateInvokerResource(toSetNumCores:Int,toSetMemory: Int): Unit = {
    myResources.numCores = toSetNumCores
    myResources.memorySize = toSetMemory

    //maxInFlightReqs_ET = 1.5 * myResources.numCores
    //maxInFlightReqs_MP = 1.0 * myResources.numCores

    maxInFlightReqsByType = maxInFlightReqsByType + ("ET" -> 1.0 * myResources.numCores)
    maxInFlightReqsByType = maxInFlightReqsByType + ("MP" -> 1.0 * myResources.numCores)

  }
  def addAction(toAddAction: String):Unit = {
    logging.info(this,s"\t <avs_debug> <AIS:addAction> Trying to add action: ${toAddAction} to allActions")
    allActions.get(toAddAction) match {
      case Some (curActStats) =>
        logging.info(this,s"\t <avs_debug> <AIS:addAction> invoker: ${id.toInt} Ok action ${toAddAction} IS present in allActions, doing nothing!")
        
      case None => 
        logging.info(this,s"\t <avs_debug> <AIS:addAction> invoker: ${id.toInt} Ok action ${toAddAction} is NOT present in allActions, adding it..")
        allActions = allActions + (toAddAction -> new ActionStatsPerInvoker(toAddAction,id.toInt,logging))
        var myActType = getActionType(toAddAction)
        // this way, I will only add it once!
        //allActionsByType(myActType)+=toAddAction //
        allActionsByType(myActType) = allActionsByType(myActType) + (toAddAction -> 1 )
    }
    var myActType = getActionType(toAddAction);
    var myStandaloneRuntime = getFunctionRuntime(toAddAction)
    logging.info(this,s"\t <avs_debug> <AIS:addAction> Action: ${toAddAction} is of type: ${myActType} and runtime: ${myStandaloneRuntime}")
  }

  def getMyMaxInFlightReqs(toCheckAction: String): Double= {
    var actType = getActionType(toCheckAction)
    maxInFlightReqsByType(actType)
  }

  def updateActionStats(toUpdateAction:String, latencyVal: Long,initTime: Long, toUpdateNumConts:Int):Unit = {
    var actType = getActionType(toUpdateAction)
    var bef_pendingReqs = inFlightReqsByType(actType)
    var after_pendingReqs = if(bef_pendingReqs > 0)  bef_pendingReqs-1 else 0
    inFlightReqsByType(actType) = after_pendingReqs // ok will have an outstanding request of my type..
    allActions.get(toUpdateAction) match {
      case Some(curActStats) => 
        curActStats.update(latencyVal,initTime,toUpdateNumConts)
        //curActStats.opZoneUpdate()
        logging.info(this,s"\t <avs_debug> <AIS:updateActionStats> 1. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} numConts: ${curActStats.numConts} movingAvgLatency: ${curActStats.movingAvgLatency} lastUpdated: ${curActStats.lastUpdated}")     
      case None =>
        //allActions = allActions + (toUpdateAction -> new ActionStatsPerInvoker(toUpdateAction,logging))
        addAction(toUpdateAction)
        var tempActStats: ActionStatsPerInvoker = allActions(toUpdateAction)
        tempActStats.update(latencyVal,initTime,toUpdateNumConts)
        logging.info(this,s"\t <avs_debug> <AIS:updateActionStats> 2. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} numConts: ${tempActStats.numConts} movingAvgLatency: ${tempActStats.movingAvgLatency} lastUpdated: ${tempActStats.lastUpdated}")     
    }
  }

  def findActionNumContsOpZone(toCheckAction: String): (Int,Int,Long) = {
    logging.info(this,s"\t <avs_debug> <AIS:findActionNumContsOpZone> 0. invoker: ${id.toInt} has action: ${toCheckAction}")             
    allActions.get(toCheckAction) match {
      case Some(curActStats) => 
        var curTime: Long = Instant.now.toEpochMilli
        var timeDiff: Long = curTime - curActStats.lastUpdated

        var resetStatsTimeoutFlag: Boolean = false
        if(curActStats.opZone == opZoneUnSafe)
          resetStatsTimeoutFlag =  timeDiff > curActStats.statsResetTimeout 

        if(  ( timeDiff > statsTimeoutInMilli) || (resetStatsTimeoutFlag) )
          curActStats.resetStats(curTime,resetStatsTimeoutFlag)

        logging.info(this,s"\t <avs_debug> <AIS:findActionNumContsOpZone> 1. invoker: ${id.toInt} has action: ${toCheckAction}, it has numConts: ${curActStats.numConts} and it's opZone: ${curActStats.opZone}")     
        (curActStats.numConts,curActStats.opZone,curActStats.lastUpdated)
      case None =>
        //allActions = allActions + (toCheckAction -> new ActionStatsPerInvoker(toCheckAction,id.toInt,logging))
        addAction(toCheckAction)
        logging.info(this,s"\t <avs_debug> <AIS:findActionNumContsOpZone> 2. invoker: ${id.toInt} does NOT have action: ${toCheckAction}.")    
        (0,opZoneSafe,Instant.now.toEpochMilli) // FIX-IT: last param is not right..
    }    
  }

  def updateActTypeStats(): Unit ={
    allActionsByType.keys.foreach{
      curActType => 
      //var allActionsOfCurType :ListBuffer[String] = allActionsByType(curActType)
      var allActionsOfCurType :mutable.Map[String,Int]= allActionsByType(curActType)
      var accumNumConts = 0; var maxOpZone = 0

      //allActionsOfCurType.foreach{ curAction =>
      allActionsOfCurType.keys.foreach{ curAction =>  
        val (numConts,thisActOpZone,lastUpdated) = findActionNumContsOpZone(curAction)
        
        if(maxOpZone < thisActOpZone)
          maxOpZone = thisActOpZone
        accumNumConts+=numConts
        logging.info(this,s"\t <avs_debug> <AIS:updateActTypeStats> actType: ${curActType} action: ${curAction}, numConts: ${numConts} accumNumConts: ${accumNumConts} opZone: ${thisActOpZone}, maxOpZone: ${maxOpZone}")    
      }
      actionTypeOpZone = actionTypeOpZone + (curActType -> maxOpZone)
      numConts = numConts + (curActType -> accumNumConts)
      logging.info(this,s"\t <avs_debug> <AIS:updateActTypeStats> For actType: ${curActType} maxOpZone: ${maxOpZone} accumNumConts: ${accumNumConts}")    
    }
  }

  def isInvokerUnsafe(): Boolean = {
    var retVal: Boolean = false
    updateActTypeStats()
    if(actionTypeOpZone("ET")==opZoneUnSafe){
      retVal = true
    }else if(actionTypeOpZone("ET")==opZoneUnSafe){
      retVal = true
    }
    retVal
  }

  def getActiveNumConts(): Int = {
    updateActTypeStats() 
    numConts("ET")+inFlightReqsByType("ET")+numConts("MP")+inFlightReqsByType("MP")
  }

  def getNumConnections(actionName:String): Int ={
    /*if(getActionType(actionName)=="ET"){
      inFlightReqsByType("ET")
    }else{
      inFlightReqsByType("MP")   
    }*/
    inFlightReqsByType("ET")+inFlightReqsByType("MP")
  }

  def checkInvokerActTypeOpZone(actionName: String,myProactiveMaxReqs: Int): (Int,Boolean) ={
    var actType = getActionType(actionName)  
    updateActTypeStats()
    var (myConts,status_opZone,lastUpdated) = findActionNumContsOpZone(actionName)
    var myTypeConts = numConts(actType)  

    var nextInvokerSpawnDecision: Boolean = false
    var curInvokerSpawnDecision: Boolean = false
    var curInvokerNumContsToSpawn = 0

    var curInstant: Long = Instant.now.toEpochMilli
    //logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ> 0. invoker: ${id.toInt} action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone}  inFlightReqsByType(actType): ${ inFlightReqsByType(actType)} maxInFlightReqsByType(actType): ${maxInFlightReqsByType(actType)}  curNumProcactiveConts: ${curNumProcactiveConts}")
    var timeSinceLastProactive = (curInstant - lastTime_ProactivelySpawned)
    var timeSinceLastUpdated = (curInstant - lastUpdated)

    if( (myTypeConts==0) && (inFlightReqsByType(actType)==1) && (timeSinceLastProactive >= statsTimeoutInMilli) ){
      nextInvokerSpawnDecision = true

      if(myConts< 0.5 * getMyMaxInFlightReqs(actionName)){
        curInvokerSpawnDecision = true
        if(myConts==0){
          curInvokerNumContsToSpawn = getMyMaxInFlightReqs(actionName).toInt - myConts - inFlightReqsByType(actType)  
        }else{
          // WARNING: Check adjustedDummyReqs calculatation in CDRBI mehtod.
          curInvokerNumContsToSpawn = getMyMaxInFlightReqs(actionName).toInt - myConts
        }
      }

      lastTime_ProactivelySpawned = curInstant
      logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ> 1. invoker: ${id.toInt} myTypeConts: ${myTypeConts} nextInvokerSpawnDecision: ${nextInvokerSpawnDecision} and timSince: ${timeSinceLastProactive} is greater than timeout: ${statsTimeoutInMilli}. So new ts: ${lastTime_ProactivelySpawned} curInvokerNumContsToSpawn: ${curInvokerNumContsToSpawn}")  
    }else if(timeSinceLastProactive >= statsTimeoutInMilli){
      nextInvokerSpawnDecision = true

      if(myConts< 0.5 * getMyMaxInFlightReqs(actionName)){
        curInvokerSpawnDecision = true
        if(myConts==0){
          curInvokerNumContsToSpawn = getMyMaxInFlightReqs(actionName).toInt - myConts - inFlightReqsByType(actType)  
        }else{
          // WARNING: Check adjustedDummyReqs calculatation in CDRBI mehtod.
          curInvokerNumContsToSpawn = getMyMaxInFlightReqs(actionName).toInt - myConts 
        }
      }      

      lastTime_ProactivelySpawned = curInstant
      logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ> 2. invoker: ${id.toInt} myTypeConts: ${myTypeConts} nextInvokerSpawnDecision: ${nextInvokerSpawnDecision} and timSince: ${timeSinceLastProactive} is greater than timeout: ${statsTimeoutInMilli}. So new ts: ${lastTime_ProactivelySpawned} curInvokerNumContsToSpawn: ${curInvokerNumContsToSpawn}")  
    }else if(timeSinceLastUpdated >= heartbeatTimeoutInMilli){
      // to ensure the proactively spawned containers in the next invoker doesn't die..
      nextInvokerSpawnDecision = true
      curInvokerSpawnDecision = true // hack, because, isProactiveNeeded wants this to check whether next invoker can accommodate more requests..
      curInvokerNumContsToSpawn = 0 // this is OK, since i don't want to send dummy reqs to myself anyway..
      logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ> 3. invoker: ${id.toInt} myTypeConts: ${myTypeConts} nextInvokerSpawnDecision: ${nextInvokerSpawnDecision} and timSince-lastUpdated: ${timeSinceLastUpdated} is greater than heartbeat-timeout: ${heartbeatTimeoutInMilli}. So new ts: ${lastTime_ProactivelySpawned} curInvokerNumContsToSpawn: ${curInvokerNumContsToSpawn}")  
    }else{
      logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ> NOPE. invoker: ${id.toInt} myTypeConts: ${myTypeConts} nextInvokerSpawnDecision: ${nextInvokerSpawnDecision} and timSince: ${timeSinceLastProactive} is greater than timeout: ${heartbeatTimeoutInMilli}. So new ts: ${lastTime_ProactivelySpawned} curInvokerNumContsToSpawn: ${curInvokerNumContsToSpawn}")  
    }
    
    if(curInvokerNumContsToSpawn>0){
      curInvokerNumContsToSpawn = if(curInvokerNumContsToSpawn > myProactiveMaxReqs) myProactiveMaxReqs else curInvokerNumContsToSpawn
      inFlightReqsByType(actType) = inFlightReqsByType(actType)+curInvokerNumContsToSpawn
      logging.info(this,s"\t <avs_debug> <AIS:cInvActOpZ:END> invoker: ${id.toInt} myTypeConts: ${myTypeConts} nextInvokerSpawnDecision: ${nextInvokerSpawnDecision} curInvokerNumContsToSpawn: ${curInvokerNumContsToSpawn}, inFlightReqsByType(actType)")  
    }
    (curInvokerNumContsToSpawn,nextInvokerSpawnDecision)
  }

  def canDummyReqBeIssued(actionName: String,numDummyReqs: Int): (Int,Boolean) = {
    // TODO: Change it to #conts instead of inFlightReqsByType?
    var actType = getActionType(actionName)
    var retVal: Boolean = false

    updateActTypeStats()
    var myTypeConts = numConts(actType)  
    var (myConts,status_opZone,lastUpdated) = findActionNumContsOpZone(actionName)
    var adjustedDummyReqs = 0
    var iterNum = 0; var maxIters = 5; var beginInFlightReqs = 0

    if(myConts==0){
      adjustedDummyReqs = ( maxInFlightReqsByType(actType).toInt - inFlightReqsByType(actType).toInt - myTypeConts)
    }else{
      // WARNING: Assuming that if I have a container, then the inflight reqs would be serviced by them. 
      //          Will definitely underestimate those scenarios where, the inflight reqs might result in spawning more containers and hence, overshooting the adjustedDummyReqs! 
      //          Hoping that these instances are rare. Ideally the proactive spawning should prevent such situations from happening apart from the warmup time.
      adjustedDummyReqs = ( maxInFlightReqsByType(actType).toInt - myTypeConts)
    }

    if(adjustedDummyReqs == 0 ){
      // no space available yo! 
      retVal = false
      logging.info(this,s"\t <avs_debug> <AIS:CDRBI> 1. A dummy action to invoker-${id.toInt} CANNOT be issued.. in invoker: ${id.toInt}. inFlightReqsByType is ${inFlightReqsByType(actType)}")
    }else if(myTypeConts<= maxInFlightReqsByType(actType)){
        // so there is some space for requests & containers.!

      adjustedDummyReqs = if(adjustedDummyReqs > numDummyReqs) numDummyReqs else adjustedDummyReqs

      if(adjustedDummyReqs>0){
        logging.info(this,s"\t <avs_debug> <AIS:CDRBI> 2. A dummy action to invoker-${id.toInt} WILL be issued.. in invoker: ${id.toInt}. inFlightReqsByType is ${inFlightReqsByType(actType)} myTypeConts: ${myTypeConts} adjustedDummyReqs: ${adjustedDummyReqs}")
        retVal = true
      }
      else{
        logging.info(this,s"\t <avs_debug> <AIS:CDRBI> 2.5 A dummy action to invoker-${id.toInt} WILL-NOT be issued.. in invoker: ${id.toInt}. inFlightReqsByType is ${inFlightReqsByType(actType)} myTypeConts: ${myTypeConts} adjustedDummyReqs: ${adjustedDummyReqs}")
        retVal = false
      }
        
    }else{
      retVal = false
      logging.info(this,s"\t <avs_debug> <AIS:CDRBI> 3. A dummy action to invoker-${id.toInt} CANNOT be issued.. in invoker: ${id.toInt}. inFlightReqsByType is ${inFlightReqsByType(actType)} myTypeConts: ${myTypeConts} adjustedDummyReqs: ${adjustedDummyReqs}")
    }

    if(adjustedDummyReqs>0){
      inFlightReqsByType(actType)= inFlightReqsByType(actType) + adjustedDummyReqs  
      logging.info(this,s"\t <avs_debug> <AIS:CDRBI> 4. Invoker-${id.toInt} inFlightReqsByType is ${beginInFlightReqs} will have ${adjustedDummyReqs} more requests. So inFlightReqsByType(actType)")
    }
    (adjustedDummyReqs,retVal)
  }

  def issuedSomeDummyReqs(actionName: String, issuedNumDummyReqs: Int): Unit = {
    var actType = getActionType(actionName)
    inFlightReqsByType(actType)+=issuedNumDummyReqs
    if( inFlightReqsByType(actType) > maxInFlightReqsByType(actType) ){
      logging.info(this,s"\t <avs_debug> <AIS:issueDummyReq> 1. DANGER DANGER In invoker-${id.toInt} in pursuit of issuing ${issuedNumDummyReqs} inFlightReqsByType: ${inFlightReqsByType(actType)} are more than the maxInFlightReqs: ${maxInFlightReqsByType(actType)} ")      
    }else{
      logging.info(this,s"\t <avs_debug> <AIS:issuedDummyReq> 2. ALL-COOL In invoker-${id.toInt} in pursuit of issuing ${issuedNumDummyReqs} inFlightReqsByType: ${inFlightReqsByType(actType)} is less than maxInFlightReqs: ${maxInFlightReqsByType(actType)} ")      
    }
  }

  def capacityRemaining(actionName:String): (Int,Boolean) = { // should update based on -- memory; #et, #mp and operating zone
    // 1. Check action-type. Alternatively, can send this as a parameter from the schedule-method
    // 2. Check whether we can accommodate this actionType (ET vs MP)? 
    //  2.a. If already a container of this action exists, ensure it is in safe opZone.
    //  2.b. If a container of this action doesn't exist, check whether we can add another container of this actionType (i.e. Check whether there are enough-cores available)
    //  Current Assumption: Ok to add a new-action, as long as all acitons are not in unsafe-region.
    
    var actType = getActionType(actionName)  
    var retVal: Boolean = false
    var (myConts,status_opZone,lastUpdated) = findActionNumContsOpZone(actionName)
    //updateActTypeStats()
    //var myTypeConts = numConts(actType)  

    logging.info(this,s"\t <avs_debug> <AIS:capRem> 0. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")
    logging.info(this,s"\t <avs_debug> <AIS:capRem> 2. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")

    if ( ( inFlightReqsByType(actType) < maxInFlightReqsByType(actType)) && (status_opZone!= opZoneUnSafe ) ){
      logging.info(this,s"\t <avs_debug> <AIS:capRem> myConts: ${myConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
      // ok, I don't have too many pending requests here..
      retVal = true
    }else{
      logging.info(this,s"\t <avs_debug> <AIS:capRem> invoker: ${id.toInt} maxInFlightReqsByType(ET): ${maxInFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} ")
      // No way JOSE!
      retVal = false
    }
    if(retVal) inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..

    logging.info(this,s"\t <avs_debug> <AIS:capRem> Final. invoker: ${id.toInt} has action: ${actionName} of type: ${actType} with retVal: ${retVal} and current pendingReqs: ${inFlightReqsByType(actType)} ")
    (inFlightReqsByType(actType),retVal)
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
