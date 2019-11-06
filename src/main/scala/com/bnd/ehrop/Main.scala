package com.bnd.ehrop

import com.typesafe.scalalogging.Logger

object Main extends App {

  protected val logger = Logger(this.getClass.getSimpleName)

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

  def get(prefix: String, args: Array[String]) = args.find(_.startsWith("-" + prefix + "=")).map(
    string => string.substring(prefix.length + 2, string.length)
  )
}

object StandardizeApp extends Standardize
object CalcFeaturesApp extends CalcFeatures