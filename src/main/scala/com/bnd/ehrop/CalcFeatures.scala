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

    val inputRootPath = withBackslash(inputPath.get)

    // check if the output file name is defined
    if (outputFileName.isEmpty) {
      logger.warn(s"The output file '-o=' not specified. Using the input path '${inputPath.get}' with 'features.csv' for the output.")
    }

    val outputFile = outputFileName.getOrElse(inputRootPath + "features.csv")

    // check if the input tables/files should be sorted by date and time-lag-based features should be calculated
    val withTimeLags = get("with_time_lags", args).map(_ => true).getOrElse(false)

    if (withTimeLags) {
      val message = "The flag 'with_time_lags' detected. Will be generating the time-lag-based features (and sorting input files by date)."
      logger.info(message)
    }

    // check if the weights calculated from dynamic scores should be exported... note that it works only if the death table/input is available
    val dynamicScoreWeightsOutputFileName = get("o-dyn_score_weights", args)

    if (dynamicScoreWeightsOutputFileName.isDefined) {
      val message = s"The output file name specified via 'o-dyn_score_weights' detected. Will be exporting the calculated weights of the dynamic scores (if death table is available) to '${dynamicScoreWeightsOutputFileName.get}'."
      logger.info(message)
    }

    val dynamicScoreWeightsInputFileName = get("i-dyn_score_weights", args)

    if (dynamicScoreWeightsInputFileName.isDefined) {
      val message = s"The input file name specified via 'i-dyn_score_weights' detected. Will be importing the calculated weights for the dynamic scores from '${dynamicScoreWeightsInputFileName.get}'."
      logger.info(message)
    }

    CalcFeaturesService.calcAndExportFeatures(
      withBackslash(inputPath.get),
      tableFeatureSpecs,
      dateIntervals,
      conceptCategories,
      scores,
      dynamicScores,
      TimeZone.getTimeZone(timeZoneCode),
      withTimeLags,
      dynamicScoreWeightsOutputFileName,
      dynamicScoreWeightsInputFileName,
      outputFile
    ) recover {
      case e: Exception =>
        logger.error(s"Error occurred: ${e.getMessage}. Exiting.")
        System.exit(1)
    }
  }
}

object CalcFeaturesService extends CalcFeaturesHelper