package com.bnd.ehrop

import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

////////////////////////
// Training FAST LANE //
////////////////////////

object CalcTrainingFastLanePersonIdCounts extends App with PersonIdCountHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/training_small/"

  calcPersonIdCountsAndOutput(rootPath)
}
