package com.bnd.ehrop

import sys.process._
import java.io.File
import java.util.{Date, TimeZone}

import _root_.akka.actor.ActorSystem
import _root_.akka.stream.{ActorMaterializer, Materializer}
import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow}
import com.bnd.ehrop.model.{Table, TableFeatures}
import com.bnd.ehrop.model.TableExt._
import org.apache.commons.io.FileUtils

import scala.concurrent.{ExecutionContext, Future}

object SortFiles extends App with AppExt {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  val inputPath = get("i", args)

  if (inputPath.isEmpty) {
    val message = "The input path '-i=' not specified. Exiting."
    logger.error(message)
    System.exit(1)
  }

  sortByDateForTableFeatures(
    withBackslash(inputPath.get),
    tableFeatureSpecs,
  ).map( _ =>
    System.exit(0)
  )

  def sortByDateForTableFeatures(
    inputPath: String,
    tableFeatures: Seq[TableFeatures],
    timeZone: TimeZone = TimeZone.getTimeZone(timeZoneCode),
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    Future.sequence(
      tableFeatureSpecs.map { tableFeatures =>
        val extraColumns = tableFeatures.extractions.flatMap(_.columns.map(_.toString)).toSet.toSeq
        val table = tableFeatures.table
        sortByDate(inputPath, table, extraColumns, timeZone, personColumnName)
      }
    )

  def sortByDate(
    inputPath: String,
    table: Table,
    dataColumnNames: Seq[String] = Nil,
    timeZone: TimeZone = TimeZone.getTimeZone(timeZoneCode),
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val inputFileName = inputPath + table.fileName
    val coreFileName = inputPath + "core-" + table.fileName
    val sortedFileName = inputPath + "sorted-" + table.fileName
    logger.info(s"Sorting of '${inputFileName}' by date started.")

    val start = new Date()

    val lineSource = AkkaFileSource.milisDateEhrDataStringCsvSource(inputFileName, personColumnName, table.dateColumn.toString, dataColumnNames, timeZone)
    AkkaFileSource.writeLines(lineSource, coreFileName).map { _ =>
      logger.info(s"Export of a core data file from '${inputFileName}' finished in ${new Date().getTime - start.getTime} ms.")

      // sort
      logger.info(s"Sorting of the core data file '${coreFileName}' by date started...")
      val sortingStart = new Date()
      (s"head -n 1 ${coreFileName}" #> new File(sortedFileName)).!
      (s"tail -n +2 ${coreFileName}" #| s"sort -t, -n -k1" #>> new File(sortedFileName)).!
      //      (s"sort -t, -n -k1 ${coreFileName}" #>> new File(sortedFileName)).!
      logger.info(s"Sorting of the core data file '${coreFileName}' by date finished in ${new Date().getTime - sortingStart.getTime} ms.")

      // delete a temp file
      FileUtils.deleteQuietly(new File(coreFileName))
    }
  }
}