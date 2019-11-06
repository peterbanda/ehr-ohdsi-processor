package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

///////////////////
// Training FULL //
///////////////////

object CalcTrainingFullFeatures extends App with CalcFullFeaturesHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/train/"

  calcAndExportFeatures(rootPath)
}
