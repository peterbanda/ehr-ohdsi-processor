package com.bnd.ehrop

object Main extends App with AppExt {

  val mode = get("mode", args)

  mode.map {
    _ match {
      case "std" => StandardizeApp.run(args)
      case "features" => CalcFeaturesApp.run(args)
      case _ =>
        val message = s"The mode option '${mode.get}' not recognized. Exiting."
        logger.error(message)
        System.exit(1)
    }
  }.getOrElse {
    val message = s"The mode option '-mode=' not specified. Defaulting to 'features'."
    logger.warn(message)
    CalcFeaturesApp.run(args)
  }
}

object StandardizeApp extends Standardize
object CalcFeaturesApp extends CalcFeatures