package com.bnd.ehrop

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object CalcFeatures extends App with CalcFullFeaturesHelper {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  def get(prefix: String) = args.find(_.startsWith("-" + prefix + "=")).map(
    string => string.substring(prefix.length + 2, string.length)
  )

  private val inputPath = get("i")

  if (inputPath.isEmpty) {
    val message = "The input path '-i=' not specified. Exiting."
    logger.error(message)
    System.exit(1)
  }

  private val outputPath = get("o").getOrElse {
    logger.warn(s"The output path '-o=' not specified. Using the input path '${inputPath.get}' for the output.")
    inputPath.get
  }

  def withBackslash(string: String) = if (string.endsWith("/")) string else string + "/"

  calcAndExportFeatures(withBackslash(inputPath.get), Some(withBackslash(outputPath)))
}
