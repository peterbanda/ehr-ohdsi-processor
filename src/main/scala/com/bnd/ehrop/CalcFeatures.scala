package com.bnd.ehrop

import java.util.TimeZone

import _root_.akka.actor.ActorSystem
import _root_.akka.stream.ActorMaterializer

trait CalcFeatures extends AppExt {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  def run(args: Array[String]) = {
    val inputPath = get("i", args)

    if (inputPath.isEmpty) {
      val message = "The input path '-i=' not specified. Exiting."
      logger.error(message)
      System.exit(1)
    }

    val outputFileName = get("o", args)

    // check if the input path exists
    fileExistsOrError(inputPath.get)

    if (outputFileName.isEmpty) {
      logger.warn(s"The output file '-o=' not specified. Using the input path '${inputPath.get}' with 'features.csv' for the output.")
    }

    // check if the input tables/files should be sorted by date
    val withDateSort = get("datesort", args).map(_ => true).getOrElse(false)

    if (withDateSort) {
      val message = "The flag 'datesort' detected. Will be sorting the input files by date."
      logger.info(message)
    }

    CalcFeaturesService.calcAndExportFeatures(
      withBackslash(inputPath.get),
      tableFeatureSpecs,
      dateIntervals,
      conceptCategories,
      TimeZone.getTimeZone(timeZoneCode),
      withDateSort,
      outputFileName
    ) recover {
      case e: Exception =>
        logger.error(s"Error occurred: ${e.getMessage}. Exiting.")
        System.exit(1)
    }
  }
}

object CalcFeaturesService extends CalcFeaturesHelper