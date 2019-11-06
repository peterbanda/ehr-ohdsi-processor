package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory

////////////////////////
// Training FAST LANE //
////////////////////////

object CalcTrainingFastLaneFeatures extends App with CalcFullFeaturesHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

//  val config = ConfigFactory.load()
//  println(config.toString)

  private val rootPath = "/home/peter/Data/ehr_dream_challenge/training_small/"

  calcAndExportFeatures(rootPath)
}
