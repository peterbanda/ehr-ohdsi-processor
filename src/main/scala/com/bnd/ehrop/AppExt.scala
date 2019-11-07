package com.bnd.ehrop

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import scala.collection.JavaConversions._

trait AppExt {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)
  protected val config = ConfigFactory.load()

  protected def fileExists(name: String) =
    new java.io.File(name).exists

  protected def fileExistsOrError(name: String) =
    if (!fileExists(name)) {
      val message = s"The input path '${name}' does not exist. Exiting."
      logger.error(message)
      System.exit(1)
    }

  def get(
    prefix: String,
    args: Array[String]
  ) = args.find(_.startsWith("-" + prefix + "=")).map(
    string => string.substring(prefix.length + 2, string.length)
  )

  lazy val dateIntervals = {
    if (!config.hasPath("dateIntervals")) {
      val message = "No 'dateIntervals' provided in application.conf. Exiting."
      logger.error(message)
      System.exit(1)
    }
    config.getObjectList("dateIntervals").map(configObject =>
      LabeledDateInterval(
        configObject.get("label").unwrapped().asInstanceOf[String],
        configObject.get("fromDaysShift").unwrapped().asInstanceOf[Int],
        configObject.get("toDaysShift").unwrapped().asInstanceOf[Int]
      )
    )
  }
}