package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/////////////////////
// Evaluation FULL //
/////////////////////

object CalcEvaluationFullPersonIdCounts extends App with PersonIdCountHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/evaluation/"

  calcPersonIdCountsAndOutput(rootPath)
}
