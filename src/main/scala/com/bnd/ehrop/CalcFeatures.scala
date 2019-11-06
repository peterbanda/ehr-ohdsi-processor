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

  private val outputFileName = get("o")

  // check if the input path exists
  fileExistsOrError(inputPath.get)

  if (outputFileName.isEmpty) {
    logger.warn(s"The output file '-o=' not specified. Using the input path '${inputPath.get}' with 'features.csv' for the output.")
  }

  def withBackslash(string: String) = if (string.endsWith("/")) string else string + "/"

  calcAndExportFeatures(withBackslash(inputPath.get), outputFileName)
}