package com.bnd.ehrop

import java.util.Date

import _root_.akka.actor.ActorSystem
import _root_.akka.stream.{ActorMaterializer, Materializer}
import _root_.akka.stream.scaladsl.{Sink, Source}
import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow, StatsAccum}

import scala.concurrent.{ExecutionContext, Future}

trait Standardize extends AppExt {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val basicNumColNames = Seq(
    "age_at_last_visit", "year_of_birth", "month_of_birth", "visit_end_date"
  )

  private val derivedNumColumnNames =
    tableFeatureSpecs.flatMap { tableFeatures =>
      val tableName = tableFeatures.table.name
      dateIntervals.flatMap( dateInterval =>
        tableFeatures.extractions.flatMap( feature =>
          if (feature.isNumeric) Some(tableName + "_" + feature.label + "_" + dateInterval.label) else None
        )
      )
    }

  def withBackslash(string: String) = if (string.endsWith("/")) string else string + "/"

  def run(args: Array[String]) = {
    val inputFileNames = get("i", args).map(_.split(",", -1).toSeq)

    if (inputFileNames.isEmpty) {
      val message = "The input file names'-i=' not specified. Exiting."
      logger.error(message)
      System.exit(1)
    }

    // check if the input files exist
    inputFileNames.get.foreach(fileExistsOrError)

    val outputFolder = get("o", args).map(withBackslash)

    if (outputFolder.isEmpty) {
      val message = "The output folder '-o=' not specified. Using the respective input folders."
      logger.warn(message)
    }

    val isOutputMeanStds = get("ostats", args).map(_ => true).getOrElse(false)

    if (isOutputMeanStds) {
      val message = "The flag 'ostats' detected. Will be outputting the means and stds into .stats file(s)."
      logger.info(message)
    }

    val inputMeanStdsFileName = get("istats", args)

    if (inputMeanStdsFileName.isDefined) {
      val message = "The input stats (means and stds) passed via 'istats'."
      logger.info(message)
    }

    {
      for {
        // calculate aggregate mean and stds across all input file
        meanStds <-
          if (inputMeanStdsFileName.isEmpty)
            calcMeanStds(inputFileNames.get, basicNumColNames ++ derivedNumColumnNames)
          else {
            logger.info(s"Parsing the stats file '${inputMeanStdsFileName.get}'.")
            AkkaFileSource.fileSource(inputMeanStdsFileName.get, "\n", true).runWith(Sink.seq).map { lines =>
              lines.map { line =>
                val els = line.split(",", -1)
                val columnName = els(0).trim
                val meanStd = if (els.length == 3) Some((els(1).trim.toDouble, els(2).trim.toDouble)) else None
                (columnName, meanStd)
              }
            }
          }

        // if requested output mean and stds
        _ = if (isOutputMeanStds) {
          val statsLines = meanStds.map { case (colName, meanStd) =>
            val els = Seq(colName) ++ meanStd.map { case (mean, std) => Seq(mean.toString, std.toString) }.getOrElse(Nil)
            els.mkString(",")
          }
          inputFileNames.get.foreach { inputFileName =>
            val outputFileName = generateOutputFileName(inputFileName, outputFolder, false) + ".stats"
            AkkaFileSource.writeLines(Source(statsLines.toList), outputFileName)
          }
        }

        // output standardized files
        _ <- standardizeAndOutputMulti(inputFileNames.get, meanStds, outputFolder)
      } yield
        System.exit(0)
    } recover {
      case e: Exception =>
        logger.error(s"Error occurred: ${e.getMessage}. Exiting.")
        System.exit(1)
    }
  }

  def standardizeAndOutputMulti(
    inputFileNames: Seq[String],
    meanStds: Seq[(String, Option[(Double, Double)])],
    outputFolder: Option[String]
  ) =
    Future.sequence(
      inputFileNames.map { inputFileName =>
        val outputFileName = generateOutputFileName(inputFileName, outputFolder, true)
        standardizeAndOutput(inputFileName, meanStds, outputFileName)
      }
    ).map(_ => ())

  private def generateOutputFileName(
    inputFileName: String,
    outputFolder: Option[String],
    withExtension: Boolean = true
  ) = {
    val lastBackslash = inputFileName.lastIndexOf("/")
    val inputPath = if (lastBackslash > 0) inputFileName.substring(0, lastBackslash + 1) else ""
    val plainName = if (lastBackslash > 0) inputFileName.substring(lastBackslash + 1, inputFileName.length) else inputFileName

    val lastDot = plainName.lastIndexOf(".")
    val fileNameStrict = if (lastDot > 0) plainName.substring(0, lastDot) else plainName
    val extension = if (lastDot > 0) plainName.substring(lastDot + 1, plainName.length) else ""

    val outputPath = outputFolder.getOrElse(inputPath)
    val outputFileWoExtension = outputPath + fileNameStrict + "-std"

    if (withExtension) outputFileWoExtension + "." + extension else outputFileWoExtension
  }

  def standardizeAndOutput(
    inputPath: String,
    columnNameWithMeanStds: Seq[(String, Option[(Double, Double)])],
    outputFileName: String
  ) = {
    val source = standardizedLineSource(inputPath, columnNameWithMeanStds)
    logger.info(s"Exporting a standardized file to '${outputFileName}'.")
    AkkaFileSource.writeLines(source, outputFileName).map { _ =>
      logger.info(s"The file '${outputFileName}' export finished.")
    }
  }

  def calcMeanStds(
    inputPaths: Seq[String],
    columnNames: Seq[String]
  ) = {
    Future.sequence(
      inputPaths.map(calcBasicStats(_, columnNames))
    ).map { multiStats =>
      multiStats.transpose.zip(columnNames).map { case (stats, columnName) =>
        // merge stats
        val mergedStats = stats.fold(StatsAccum(0, 0, 0)) { case (total, accum) =>
          StatsAccum(
            total.sum + accum.sum,
            total.sqSum + accum.sqSum,
            total.count + accum.count
          )
        }
        (columnName, AkkaFlow.calcMeanStd(mergedStats))
      }
    }
  }

  private def calcBasicStats(
    inputPath: String,
    columnNames: Seq[String])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val doubleSource = doubleCsvSource(inputPath, columnNames)

    doubleSource.via(AkkaFlow.calcBasicStats(columnNames.size)).runWith(Sink.head).map { stats =>
      logger.info(s"Basic stats for '${inputPath}' and ${columnNames.size} columns calculated in ${new Date().getTime - start.getTime} ms.")
      stats
    }
  }

  private def doubleCsvSource(
    inputPath: String,
    columnNames: Seq[String]
  ) =
    AkkaFileSource.csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val columnIndeces = columnNames.map(columnName =>
          columnIndexMap.get(columnName).getOrElse {
            val message = s"Column '${columnName}' in the file '${inputPath}' not found"
            throw new IllegalArgumentException(message)
          }
        )

        def values(els: Array[String]) =
          columnIndeces.map { index =>
            val string = els(index).trim
            if (string.nonEmpty) Some(string.toDouble) else None
          }

        els => values(els)
      }
    )

  private def standardizedLineSource(
    inputPath: String,
    columnNameWithMeanStds: Seq[(String, Option[(Double, Double)])]
  ) =
    AkkaFileSource.csvAsStringSourceWithTransformAndHeader(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val indexMeanStdMap: Map[Int, (Double, Double)] = columnNameWithMeanStds.flatMap { case (columnName, meanStd) =>
          meanStd.map { meanStd =>
            val colIndex = columnIndexMap.get(columnName).getOrElse {
              val message = s"Column '${columnName}' in the file '${inputPath}' not found"
              throw new IllegalArgumentException(message)
            }
            (colIndex, meanStd)
          }
        }.toMap

        def asString(el: String) = {
          val string = el.trim
          if (string.nonEmpty) Some(string) else None
        }

        els => els.zipWithIndex.map { case (el, index) =>
          asString(el).map { string =>
            indexMeanStdMap.get(index) match {
              case Some((mean, std)) =>
                val double = string.toDouble
                if (std != 0) {
                  val standValue = (double - mean) / std
                  (math rint standValue * 10000) / 10000 // round to 4 decimal places
                } else "0"

              case None => string
            }
          }.getOrElse("")
        }.mkString(",")
      }
    )
}