package com.bnd.ehrop

import com.typesafe.scalalogging.Logger

trait AppExt {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)

  protected def fileExists(name: String) =
    new java.io.File(name).exists

  protected def fileExistsOrError(name: String) =
    if (!fileExists(name)) {
      val message = s"The input path '${name}' does not exist. Exiting."
      logger.error(message)
      System.exit(1)
    }

  def get(prefix: String, args: Array[String]) = args.find(_.startsWith("-" + prefix + "=")).map(
    string => string.substring(prefix.length + 2, string.length)
  )
}