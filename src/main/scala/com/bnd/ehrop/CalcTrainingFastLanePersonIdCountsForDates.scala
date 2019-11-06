package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

////////////////////////
// Training FAST LANE //
////////////////////////

object CalcTrainingFastLanePersonIdCountsForDates extends App with PersonIdCountHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/training_small/"

  private val fromDate = defaultDateFormat.parse("2008-01-01")
  private val toDate = defaultDateFormat.parse("2009-01-01")
  private val idDateMap = (0 to 200000).map( id => (id, (fromDate, toDate))).toMap

  calcPersonIdDateCountsAndOutput(rootPath, idDateMap)
}
