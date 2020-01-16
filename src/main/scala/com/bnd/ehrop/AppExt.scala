package com.bnd.ehrop

import java.util
import java.util.TimeZone

import com.bnd.ehrop.model._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConversions._

trait AppExt extends BasicHelper {

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

        def asString(fieldName: String) = extractionObject.get(fieldName).unwrapped().asInstanceOf[String]
        def conceptColumnString = asString("conceptColumn")
        def columnString = asString("column")

        extractionType match {
          case "Count" =>
            Count[table.Col]()

          case "DistinctCount" =>
            DistinctCount[table.Col](withName(columnString))

          case "Sum" =>
            Sum[table.Col](withName(columnString))

          case "LastDefinedConcept" =>
            LastDefinedConcept[table.Col](withName(conceptColumnString))

          case "ConceptCategoryExists" =>
            ConceptCategoryExists[table.Col](
              withName(conceptColumnString),
              extractionObject.get("categoryName").unwrapped().asInstanceOf[String]
            )

          case "ConceptCategoryCount" =>
            ConceptCategoryCount[table.Col](
              withName(conceptColumnString),
              extractionObject.get("categoryName").unwrapped().asInstanceOf[String]
            )

          case "ConceptCategoryIsLastDefined" =>
            ConceptCategoryIsLastDefined[table.Col](
              withName(conceptColumnString),
              extractionObject.get("categoryName").unwrapped().asInstanceOf[String]
            )

          case "DurationFromFirst" =>
            DurationFromFirst[table.Col]()

          case _ => throw new IllegalArgumentException(s"Feature extraction type '${extractionType}' not recognized.")
        }
      }

      TableFeatures(table)(extractions :_*)
    }
  }

  lazy val conceptCategories: Seq[ConceptCategory] = {
    if (!config.hasPath("conceptCategories")) {
      val message = "No 'conceptCategories' provided in application.conf."
      logger.warn(message)
      Nil
    } else {
      config.getObjectList("conceptCategories").map(configObject =>
        ConceptCategory(
          configObject.get("name").unwrapped().asInstanceOf[String],
          configObject.get("conceptIds").unwrapped().asInstanceOf[util.ArrayList[Int]].toSeq.toSet,
        )
      )
    }
  }

  lazy val scores: Seq[Score] = {
    if (!config.hasPath("scores")) {
      val message = "No 'scores' provided in application.conf."
      logger.warn(message)
      Nil
    } else {
      config.getObjectList("scores").map(configObject =>
        Score(
          configObject.get("name").unwrapped().asInstanceOf[String],
          configObject.toConfig.getObjectList("elements").map { elementObject =>
            ScoreElement(
              elementObject.get("categoryNames").unwrapped().asInstanceOf[util.ArrayList[String]].toSeq,
              elementObject.get("weight").unwrapped().asInstanceOf[Int],
            )
          }
        )
      )
    }
  }

  val timeZoneCode = {
    if (!config.hasPath("timeZone.code")) {
      val message = "No 'timeZone.code' provided in application.conf. Exiting."
      logger.error(message)
      System.exit(1)
    }

    config.getString("timeZone.code")
  }
}

trait BasicHelper {

  protected val logger = Logger(this.getClass.getSimpleName)
  protected val defaultTimeZone = TimeZone.getTimeZone("CET") // CEST

  def withBackslash(string: String) = if (string.endsWith("/")) string else string + "/"
}