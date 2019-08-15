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
//import scala.collection.immutable.ListMap // avs
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
  var resetNumInstances = 2 
  var minResetTimeInMilli = 3*1000
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
  var lastUpdated: Long = Instant.now.toEpochMilli // TODO: should be time, will update TYPE later.
  
  def simplePrint(toPrintAction:String, toPrintLatency: Long, toPrintNumConts:Int): Unit = {
   logging.info(this,s"\t <avs_debug> <simplePrint> <ASPI> Action: ${toPrintAction} has averageLatency: ${toPrintLatency} and #conts: ${toPrintNumConts}") 
  }

  def updateOpZone(): Unit = {
    var toSetOpZone = opZone
    var latencyRatio: Double = (movingAvgLatency.toDouble/standaloneRuntime)
    var toleranceRatio: Double = if(latencyRatio > 1.0) ((latencyRatio-1)/(latencyTolerance-1)) else safeBeginThreshold
    logging.info(this,s"\t <avs_debug> <ASPI> In updateOpZone of Action: ${actionName} and curOpZone is ${opZone} movingAvgLatency: ${movingAvgLatency} and standaloneRuntime: ${standaloneRuntime} and latencyRatio: ${latencyRatio} and toleranceRatio: ${toleranceRatio}") 

    if( (toleranceRatio >= safeBeginThreshold) && (toleranceRatio <= safeEndThreshold)){
        opZone = opZoneSafe
        logging.info(this,s"\t <avs_debug> <ASPI> In updateOpZone of Action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${safeBeginThreshold} and end: ${safeEndThreshold}, so opzone is SAFE --${opZone}.") 
    }else if( (toleranceRatio >= warnBeginThreshold) && (toleranceRatio <= warnEndThreshold)){
      opZone = opZoneWarn
      logging.info(this,s"\t <avs_debug> <ASPI> In updateOpZone of Action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${warnBeginThreshold} and end: ${warnEndThreshold}, so opzone is in WARNING safe --${opZone}") 
    }else if( (toleranceRatio >= unsafeBeginThreshold) && (toleranceRatio <= unsafeEndThreshold)){
      opZone = opZoneUnSafe
      logging.info(this,s"\t <avs_debug> <ASPI> In updateOpZone of Action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently less than, begin: ${unsafeBeginThreshold} and end: ${unsafeEndThreshold}, so opzone is UNSAFE --${opZone}") 
    }else{
      opZone = opZoneUnSafe
      logging.info(this,s"\t <avs_debug> <ASPI> In updateOpZone of Action: ${actionName}, myInvokerID: ${myInvokerID} latencyRatio: ${latencyRatio} toleranceRatio: ${toleranceRatio} is evidently in a weird region, so it should be declared UNSAFE --${opZone}") 
    }
  }

  def resetStats(curTime: Long): Unit = {
    // Assuming that, if either it is not updated in the past, 
    // a. statsTimeoutInMilli : Atleast a container would have died.
    // b. statsResetTimeout: It would have passed some time, so, the load should have subsided..

    logging.info(this,s"\t <avs_debug> <ASPI> <resetStats> In updateOpZone of Action: ${actionName}, myInvokerID: ${myInvokerID} resetting my stats since: curTime: ${curTime} is larger than lastUpdated: ${lastUpdated} by ${statsTimeoutInMilli} or ${statsResetTimeout} opZone: ${opZone}") 
    opZone = 0 
    movingAvgLatency = 0
    numConts = 0
    cumulSum = 0
    runningCount = 0
    lastUpdated = Instant.now.toEpochMilli
    // EXPT-WARNING: Might be better to issue a load request and refresh stats!, instead of resetting willy nilly!
  }

  // update(latencyVal,initTime,toUpdateNumConts)
  def update(latency: Long, initTime: Long, toUpdateNumConts: Int): Unit = {
    if(initTime==0){ // warm starts only!!
      cumulSum+= latency  
      runningCount+= 1
      lastUpdated = Instant.now.toEpochMilli      
    }
    numConts = toUpdateNumConts
    movingAvgLatency = if(runningCount>0) (cumulSum/runningCount) else 0
    updateOpZone()
    logging.info(this,s"\t <avs_debug> <ASPI> <update> In update of Action: ${actionName} myInvokerID: ${myInvokerID}, latency: ${latency} count: ${runningCount} cumulSum: ${cumulSum} movingAvgLatency: ${movingAvgLatency} numConts: ${numConts} ")     
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

  def getUsedInvoker(): Option[InvokerInstanceId] = {

    //var blah = cmplxLastInstUsed.toSeq.sortBy(curEle => (curEle._2.numInFlightReqs,curEle._2.lastUsed))(Ordering[(Int,Long)].reverse)
    var blah = cmplxLastInstUsed.toSeq.sortBy(curEle => (curEle._2.invokerRank)) //(Ordering[(Int,Long)].reverse)

    blah.foreach{
      case (curInvoker,curInvokerRunningState) => 
        //val curInvokerRunningState: InvokerRunningState = cmplxLastInstUsed(curInvoker)
        logging.info(this,s"\t <avs_debug> <CBGetUsedInvoker> Action: ${actionName}, invoker: ${curInvokerRunningState.invokerRank} was the invoker used at ${curInvokerRunningState.lastUsed} and had #inflight-reqs: ${curInvokerRunningState.numInFlightReqs} ")
        logging.info(this,s"\t <avs_debug> <getUsedInvoker> Action: ${actionName}, invoker: ${curInvoker.toInt} checking whether it has any capacityRemaining...")
        // If I fit, I will choose this.
        // TODO: Change this so that I iterate based on some "ranking"
        var curInvokerStats: AdapativeInvokerStats = usedInvokers(curInvoker)
        val (numInFlightReqs,decision) = curInvokerStats.capacityRemaining(actionName)
        if(decision){
          var curInstant: Long = Instant.now.toEpochMilli
          lastInstantUsed(curInvoker) = curInstant

          cmplxLastInstUsed(curInvoker).numInFlightReqs = numInFlightReqs
          cmplxLastInstUsed(curInvoker).lastUsed = curInstant

          logging.info(this,s"\t <avs_debug> <getUsedInvoker> Invoker: ${curInvoker.toInt} supposedly has capacity, am I yielding at instant: ${curInstant}, lastInstantUsed(curInvoker): ${lastInstantUsed(curInvoker)}")  
          return Some(curInvoker)
        }      
    }

    //ListMap(cmplxLastInstUsed.toSeq.sortBy(curEle => (curEle._2.numInFlightReqs,curEle._2.lastUsed))(Ordering[(Int,Long)].reverse): _*).keys().foreach{
    //  curInvoker =>
    //  logging.info(this,s"\t <avs_debug> <CBGetUsedInvoker> Action: ${actionName}, invoker: ${curInvoker.toInt} was the invoker used at ${cmplxLastInstUsed(curInvoker).lastUsed} and had #inflight-reqs: ${cmplxLastInstUsed(curInvoker).numInFlightReqs}")
    //}

    //ListMap(cmplxLastInstUsed.toSeq.sortBy(_._2.numInFlightReqs,_._2.lastUsed)(Ordering[Int].reverse):_*)
    //ListMap(cmplxLastInstUsed.toSeq.sortBy( cmplxLastInstUsed.toSeq => (_._2.numInFlightReqs,_._2.lastUsed) )(Ordering[Long].reverse)).keys().foreach{
    //}

    /*ListMap(lastInstantUsed.toSeq.sortWith(_._2 > _._2):_*).keys.foreach{
      curInvoker => 
      logging.info(this,s"\t <avs_debug> <getUsedInvoker> Action: ${actionName}, invoker: ${curInvoker.toInt} was the invoker used at ${lastInstantUsed(curInvoker)}")
      usedInvokers.get(curInvoker) match {
        case Some(curInvokerStats) => 
          logging.info(this,s"\t <avs_debug> <getUsedInvoker> Action: ${actionName}, invoker: ${curInvoker.toInt} checking whether it has any capacityRemaining...")
          // If I fit, I will choose this.
          // TODO: Change this so that I iterate based on some "ranking"
          val (numInFlightReqs,decision) = curInvokerStats.capacityRemaining(actionName)
          if(decision){
            var curInstant: Long = Instant.now.toEpochMilli
            lastInstantUsed(curInvoker) = curInstant

            cmplxLastInstUsed(curInvoker).numInFlightReqs = numInFlightReqs
            cmplxLastInstUsed(curInvoker).lastUsed = curInstant

            logging.info(this,s"\t <avs_debug> <getUsedInvoker> Invoker: ${curInvoker.toInt} supposedly has capacity, am I yielding at instant: ${curInstant}, lastInstantUsed(curInvoker): ${lastInstantUsed(curInvoker)}")  
            return Some(curInvoker)
          }
        case None =>
          logging.info(this,s"\t <avs_debug> <getUsedInvoker> Invoker: ${curInvoker.toInt}'s AdapativeInvokerStats object, not yet passed onto the action. So, not doing anything with it..")
      }
    }*/

    logging.info(this,s"\t <avs_debug> <getUsedInvoker> Action: ${actionName} did not get a used invoker :( :( ")
    None

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
  // -------------- Thresholds --------------

  def updateInvokerResource(toSetNumCores:Int,toSetMemory: Int): Unit = {
    myResources.numCores = toSetNumCores
    myResources.memorySize = toSetMemory

    //maxInFlightReqs_ET = 1.5 * myResources.numCores
    //maxInFlightReqs_MP = 1.0 * myResources.numCores

    maxInFlightReqsByType = maxInFlightReqsByType + ("ET" -> 1.5 * myResources.numCores)
    maxInFlightReqsByType = maxInFlightReqsByType + ("MP" -> 1.0 * myResources.numCores)

  }
  def addAction(toAddAction: String):Unit = {
    logging.info(this,s"\t <avs_debug> <AIS> <addAction> Trying to add action: ${toAddAction} to allActions")
    allActions.get(toAddAction) match {
      case Some (curActStats) =>
        logging.info(this,s"\t <avs_debug> <AIS> <addAction> invoker: ${id.toInt} Ok action ${toAddAction} IS present in allActions, doing nothing!")
        
      case None => 
        logging.info(this,s"\t <avs_debug> <AIS> <addAction> invoker: ${id.toInt} Ok action ${toAddAction} is NOT present in allActions, adding it..")
        allActions = allActions + (toAddAction -> new ActionStatsPerInvoker(toAddAction,id.toInt,logging))
        var myActType = getActionType(toAddAction)
        // this way, I will only add it once!
        //allActionsByType(myActType)+=toAddAction //
        allActionsByType(myActType) = allActionsByType(myActType) + (toAddAction -> 1 )
    }
    var myActType = getActionType(toAddAction);
    var myStandaloneRuntime = getFunctionRuntime(toAddAction)
    logging.info(this,s"\t <avs_debug> <AIS> <addAction> Action: ${toAddAction} is of type: ${myActType} and runtime: ${myStandaloneRuntime}")
  }

  def updateActionStats(toUpdateAction:String, latencyVal: Long,initTime: Long, toUpdateNumConts:Int):Unit = {
    var actType = getActionType(toUpdateAction)
    var bef_pendingReqs = inFlightReqsByType(actType)
    var after_pendingReqs = if(bef_pendingReqs > 0)  bef_pendingReqs-1 else 0
    inFlightReqsByType(actType) = after_pendingReqs // ok will have an outstanding request of my type..
    allActions.get(toUpdateAction) match {
      case Some(curActStats) => 
        curActStats.update(latencyVal,initTime,toUpdateNumConts)
        //curActStats.updateOpZone()
        logging.info(this,s"\t <avs_debug> <AIS> <updateActionStats> 1. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} numConts: ${curActStats.numConts} movingAvgLatency: ${curActStats.movingAvgLatency} lastUpdated: ${curActStats.lastUpdated}")     
      case None =>
        //allActions = allActions + (toUpdateAction -> new ActionStatsPerInvoker(toUpdateAction,logging))
        addAction(toUpdateAction)
        var tempActStats: ActionStatsPerInvoker = allActions(toUpdateAction)
        tempActStats.update(latencyVal,initTime,toUpdateNumConts)
        logging.info(this,s"\t <avs_debug> <AIS> <updateActionStats> 2. invoker: ${id.toInt} bef-pendingReqs: ${bef_pendingReqs} aft-: ${inFlightReqsByType(actType)} action: ${toUpdateAction} numConts: ${tempActStats.numConts} movingAvgLatency: ${tempActStats.movingAvgLatency} lastUpdated: ${tempActStats.lastUpdated}")     
    }
  }

  def findActionNumContsOpZone(toCheckAction: String): (Int,Int) = {
    logging.info(this,s"\t <avs_debug> <AIS> <findActionNumContsOpZone> 0. invoker: ${id.toInt} has action: ${toCheckAction}")             
    allActions.get(toCheckAction) match {
      case Some(curActStats) => 
        var curTime: Long = Instant.now.toEpochMilli
        var timeDiff: Long = curTime - curActStats.lastUpdated
        if(  ( timeDiff > statsTimeoutInMilli) || ( timeDiff > curActStats.statsResetTimeout ) )
          curActStats.resetStats(curTime)

        logging.info(this,s"\t <avs_debug> <AIS> <findActionNumContsOpZone> 1. invoker: ${id.toInt} has action: ${toCheckAction}, it has numConts: ${curActStats.numConts} and it's opZone: ${curActStats.opZone}")     
        (curActStats.numConts,curActStats.opZone)
      case None =>
        //allActions = allActions + (toCheckAction -> new ActionStatsPerInvoker(toCheckAction,id.toInt,logging))
        addAction(toCheckAction)
        logging.info(this,s"\t <avs_debug> <AIS> <findActionNumContsOpZone> 2. invoker: ${id.toInt} does NOT have action: ${toCheckAction}.")    
        (0,opZoneSafe)
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
        val (numConts,thisActOpZone) = findActionNumContsOpZone(curAction)
        
        if(maxOpZone < thisActOpZone)
          maxOpZone = thisActOpZone
        accumNumConts+=numConts
        logging.info(this,s"\t <avs_debug> <AIS> <updateActTypeStats> actType: ${curActType} action: ${curAction}, numConts: ${numConts} accumNumConts: ${accumNumConts} opZone: ${thisActOpZone}, maxOpZone: ${maxOpZone}")    
      }
      actionTypeOpZone = actionTypeOpZone + (curActType -> maxOpZone)
      numConts = numConts + (curActType -> accumNumConts)
      logging.info(this,s"\t <avs_debug> <AIS> <updateActTypeStats> For actType: ${curActType} maxOpZone: ${maxOpZone} accumNumConts: ${accumNumConts}")    
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

  def checkInvokerActTypeOpZone(actionName: String): Boolean ={
    var actType = getActionType(actionName)  
    //updateActTypeStats()
    var (myConts,status_opZone) = findActionNumContsOpZone(actionName)
    var myTypeConts = numConts(actType)  

    var retVal: Boolean = false
    logging.info(this,s"\t <avs_debug> <AIS> <cInvActOpZ> 0. invoker: ${id.toInt} action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone}  inFlightReqsByType(actType): ${ inFlightReqsByType(actType)} maxInFlightReqsByType(actType): ${maxInFlightReqsByType(actType)}  curNumProcactiveConts: ${curNumProcactiveConts}")

    if( inFlightReqsByType(actType) > (warningZoneThd *maxInFlightReqsByType(actType) ) ){
      logging.info(this,s"\t <avs_debug> <AIS> <cInvActOpZ> 1. invoker: ${id.toInt} inFlightReqsByType(actType): ${ inFlightReqsByType(actType)} is atleast ${warningZoneThd*maxInFlightReqsByType(actType)} (warningZoneThd*maxInFlightReqsByType(actType)) curNumProcactiveConts: ${curNumProcactiveConts}")
      retVal = true
      curNumProcactiveConts+=1
    }else if(myTypeConts > (warningZoneThd*maxInFlightReqsByType(actType))){
      logging.info(this,s"\t <avs_debug> <AIS> <cInvActOpZ> 2. invoker: ${id.toInt} myTypeConts: ${ myTypeConts} is atleast ${warningZoneThd*maxInFlightReqsByType(actType)} (warningZoneThd*maxInFlightReqsByType(actType))  curNumProcactiveConts: ${curNumProcactiveConts}")
      retVal = true      
      curNumProcactiveConts+=1
    }else{
      curNumProcactiveConts = 0
      logging.info(this,s"\t <avs_debug> <AIS> <cInvActOpZ> 3. invoker: ${id.toInt} both conditions are neutralized, resetting curNumProcactiveConts: ${curNumProcactiveConts}")
    }
    if(curNumProcactiveConts>maxProactiveNumConts){
      logging.info(this,s"\t <avs_debug> <AIS> <cInvActOpZ> 4. invoker: ${id.toInt} maxed out with proactivism, since curNumProcactiveConts: ${curNumProcactiveConts} is larger than maxProactiveNumConts")
      retVal = false
    }
    retVal
  }

  def canDummyReqBeIssued(actionName: String): Boolean = {
    
    var actType = getActionType(actionName)
    var retVal: Boolean = false

    if(inFlightReqsByType(actType) < maxInFlightReqsByType(actType)){
      inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..
      logging.info(this,s"\t <avs_debug> <AIS> <issuedDummyReq> 1. A dummy action of ${id.toInt} shall be issued.. in invoker: ${id.toInt}. Updating inFlightReqsByType to ${inFlightReqsByType(actType)}")
      retVal = true
    }else{
      logging.info(this,s"\t <avs_debug> <AIS> <issuedDummyReq> 2. A dummy action of ${id.toInt} CANNOT be issued.. in invoker: ${id.toInt}. inFlightReqsByType is ${inFlightReqsByType(actType)}")
      retVal = false
    }
    retVal
  }

  def capacityRemaining(actionName:String): (Int,Boolean) = { // should update based on -- memory; #et, #mp and operating zone
    // 1. Check action-type. Alternatively, can send this as a parameter from the schedule-method
    // 2. Check whether we can accommodate this actionType (ET vs MP)? 
    //  2.a. If already a container of this action exists, ensure it is in safe opZone.
    //  2.b. If a container of this action doesn't exist, check whether we can add another container of this actionType (i.e. Check whether there are enough-cores available)
    //  Current Assumption: Ok to add a new-action, as long as all acitons are not in unsafe-region.
    
    var actType = getActionType(actionName)  
    var retVal: Boolean = false
    var (myConts,status_opZone) = findActionNumContsOpZone(actionName)
    updateActTypeStats()
    var myTypeConts = numConts(actType)  

    logging.info(this,s"\t <avs_debug> <AIS> <capRem> 0. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")

    if(actType == "MP"){

      logging.info(this,s"\t <avs_debug> <AIS> <capRem> 1. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} and myTypeConts: ${myTypeConts} ")

      if( (inFlightReqsByType(actType) < maxInFlightReqsByType(actType)) && (status_opZone!= opZoneUnSafe)) {
        // Ok, I can accommodate atleast one request..
        logging.info(this,s"\t <avs_debug> <AIS> <capRem> <MP-0> myTypeConts: ${myTypeConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
        if( (myTypeConts < myResources.numCores)){
          // ok, even if I need one more container, it can be fit in, I guess!
          // Even if there is space, if I am in unsafe region, I will go somewhere else (EXPT-WARNING: this will likely make me use more machines!)
          // (EXPT-WARNING: Am also sending a request, if it is in warning zone -- this could hurt the latency SLO)
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <MP-1.1> myTypeConts: ${myTypeConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
          retVal = true                     
        }else if((myTypeConts == myResources.numCores) && (myConts!=0) && (status_opZone == opZoneSafe)){
          // ok, I have atleast one container, and all of them are in safe-zone (EXPT-WARNING: this is a bit suspect).
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <MP-1.2> myTypeConts: ${myTypeConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
        }else{
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <MP-1.3> myTypeConts: ${myTypeConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} myConts: ${myConts} status_opZone: ${status_opZone}")
          // No way JOSE!
          retVal = false
        }
      }
    }
    else if(actType == "ET"){
      logging.info(this,s"\t <avs_debug> <AIS> <capRem> 2. invoker: ${id.toInt} has action: ${actionName}, myConts: ${myConts}, opZone: ${status_opZone} ")
      if ( ( inFlightReqsByType(actType) < maxInFlightReqsByType(actType)) && (status_opZone!= opZoneUnSafe ) ){
        logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-0> myTypeConts: ${myTypeConts} pendingReqs: ${inFlightReqsByType(actType)} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
        // ok, I don't have too many pending requests here..

        // If I don't have any more containers (myTypeConts==0) of my type, I will try not to accept utmost 1 request!
        /*if( (myTypeConts==0) && (inFlightReqsByType(actType) > myTypeConts) ){
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-0.5> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
          retVal = false
        }*/
        if( (myTypeConts < myResources.numCores) ){
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-1> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
          // ok, I have atleast one of my own containers (and not in unsafe region) and atmost as many containers as numCores.
          // (EXPT-WARNING: Am also sending a request, if it is in warning zone -- this could hurt the latency SLO)
          retVal = true
        }else if( myTypeConts <= inFlightReqsByType(actType) ){
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-2> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone}")
          // we have maxed out, making sure everybody is safe atleast.
          var myActTypeOpZone = actionTypeOpZone("ET")
          if( myActTypeOpZone <= opZoneSafe){ // WARNING: by checking for maxOpZone to be safe, it already covers by own operating type.
            logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-2.1> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myActTypeOpZone: ${myActTypeOpZone} ")
            // EXPT-WARNING: This could make me use a new machine, during init-zone of the experiments..
            retVal = true
          }else{ // Likely atleast one of them is in warning atleast
            if( (myConts!=0) && (status_opZone!= opZoneUnSafe) ){
              logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-2.2> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} myActTypeOpZone: ${myActTypeOpZone} ")
              // I have atleast one container and it is atmost in warning zone, so will allow it..
              retVal = true
            }else{
              logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-2.2-Inv> myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} myActTypeOpZone: ${myActTypeOpZone} ")
              // this doesn't look good bro, so will just not accept this request!
              retVal = false  
            }
            retVal = false
          }
        }else{
          logging.info(this,s"\t <avs_debug> <AIS> <capRem> <ET-2-Inv> invoker: ${id.toInt} maxInFlightReqsByType(ET): ${maxInFlightReqsByType(actType)} myTypeConts: ${myTypeConts} numCores: ${myResources.numCores} status_opZone: ${status_opZone} myConts: ${myConts} ")
          // No way JOSE! Can't have > twice the num of ET containers.
          retVal = false
        }
      }

    }else{
      logging.info(this,s"\t <avs_debug> <AIS> <capRem> actType neither MP or ET, HANDLE it!")
      // shouldn't come here, but putting it here just in case..
      retVal = false
    }
    if(retVal) inFlightReqsByType(actType) = inFlightReqsByType(actType)+1  // ok will have an outstanding request of my type..

    logging.info(this,s"\t <avs_debug> <AIS> <capRem> Final. invoker: ${id.toInt} has action: ${actionName} of type: ${actType} with retVal: ${retVal} and current pendingReqs: ${inFlightReqsByType(actType)} ")
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
