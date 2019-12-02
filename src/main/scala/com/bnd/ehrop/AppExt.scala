package com.bnd.ehrop

import com.bnd.ehrop.model._
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

  lazy val dateIntervals: Seq[DayInterval] = {
    if (!config.hasPath("dateIntervals")) {
      val message = "No 'dateIntervals' provided in application.conf. Exiting."
      logger.error(message)
      System.exit(1)
    }
    config.getObjectList("dateIntervals").map(configObject =>
      DayInterval(
        configObject.get("label").unwrapped().asInstanceOf[String],
        configObject.get("fromDaysShift").unwrapped().asInstanceOf[Int],
        configObject.get("toDaysShift").unwrapped().asInstanceOf[Int]
      )
    )
  }

  lazy val tableFeatureSpecs: Seq[TableFeatures] = {
    if (!config.hasPath("tableFeaturesSpecs")) {
      val message = "No 'tableFeaturesSpecs' provided in application.conf. Exiting."
      logger.error(message)
      System.exit(1)
    }

    config.getObjectList("tableFeaturesSpecs").map { tableObject =>
      val tableName = tableObject.get("table").unwrapped().asInstanceOf[String]
      val tableOption = Table.values.find(_.name == tableName)

      if (tableOption.isEmpty) {
        val message = s"Table '${tableName}' found in application.conf not recognized. Exiting."
        logger.error(message)
        System.exit(1)
      }

      if (!tableObject.toConfig.hasPath("extractions")) {
        val message = s"No 'extractions' provided for the table '${tableName}' in application.conf. Exiting."
        logger.error(message)
        System.exit(1)
      }

      val table: Table = tableOption.get
      val enumeration: Enumeration = tableOption.get
      def withName(columnName: String) = enumeration.withName(columnName).asInstanceOf[table.Col]

      val extractions: Seq[FeatureExtraction[table.Col]] = tableObject.toConfig.getObjectList("extractions").map { extractionObject =>
        val extractionType = extractionObject.get("type").unwrapped().asInstanceOf[String]
        def conceptColumnString = extractionObject.get("conceptColumn").unwrapped().asInstanceOf[String]

        extractionType match {
          case "Count" =>
            Count[table.Col]()

          case "DistinctCount" =>
            DistinctCount[table.Col](withName(conceptColumnString))

          case "LastDefinedConcept" =>
            LastDefinedConcept[table.Col](withName(conceptColumnString))

          case "ExistConceptInGroup" =>
            ExistConceptInGroup[table.Col](
              withName(conceptColumnString),
              extractionObject.get("ids").unwrapped().asInstanceOf[Seq[Int]].toSet,
              extractionObject.get("groupName").unwrapped().asInstanceOf[String]
            )
        }
      }

      TableFeatures(table)(extractions :_*)
    }
  }
}