package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

//////////////////////////
// Evaluation FAST LANE //
//////////////////////////

object CalcEvaluationFastLaneFeatures extends App with CalcFullFeaturesHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/evaluation_small/"

  calcAndExportFeatures(rootPath)
}
