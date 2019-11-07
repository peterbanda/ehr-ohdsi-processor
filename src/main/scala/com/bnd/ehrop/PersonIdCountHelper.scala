package com.bnd.ehrop

import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import java.util.{Calendar, Date}

import org.ada.server.akka.AkkaStreamUtil
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}

trait PersonIdCountHelper {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)

  protected val defaultDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def calcPersonIdCountsAndOutput(
    rootPath: String)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Unit = {
    val inputs = IOSpec.counts(rootPath)
    calcPersonIdCountsAndOutput(
      inputs,
      rootPath + "person_id_counts.csv"
    )
  }

  def calcPersonIdDateCountsAndOutput(
    rootPath: String,
    idFromToDatesMap: Map[Int, (Date, Date)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Unit = {
    val inputs = IOSpec.dateCounts(rootPath)

    calcPersonIdDateCountsAndOutput(
      inputs,
      idFromToDatesMap,
      rootPath + "person_id_counts.csv"
    )
  }

  def calcPersonIdCountsAndOutput(
    inputs: Seq[(String, String)],
    outputFileName: String)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = calcPersonIdCounts(inputs).foreach { case (columnNames, groupCounts) =>
    saveToFile(columnNames, groupCounts, outputFileName)
    System.exit(0)
  }

  def calcPersonIdDateCountsAndOutput(
    inputs: Seq[(String, String, String)],
    idFromToDatesMap: Map[Int, (Date, Date)],
    outputFileName: String)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = calcPersonIdDateCounts(inputs, idFromToDatesMap).foreach { case (columnNames, groupCounts) =>
    saveToFile(columnNames, groupCounts, outputFileName)
    System.exit(0)
  }

  //////////////////
  // Calc for ALL //
  //////////////////

  def calcPersonIdCountsAll(
    rootPath: String)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    calcPersonIdCounts(IOSpec.counts(rootPath))

  def calcPersonIdDateCountsAll(
    rootPath: String,
    idFromToDatesMap: Map[Int, (Date, Date)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    calcPersonIdDateCounts(IOSpec.dateCounts(rootPath), idFromToDatesMap)

  def calcPersonIdDateCountsAll(
    rootPath: String,
    idFromToDatesMaps: Seq[Map[Int, (Date, Date)]],
    outputSuffixes: Seq[String])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val paths = IOSpec.dateCounts(rootPath)

    val pathsWithOutputs = paths.map { case (path, dateColumn, outputColName) =>
      val outputCols = outputSuffixes.map(suffix => outputColName + "_" + suffix)

      (path, dateColumn, outputCols)
    }

    calcPersonIdDateMultiCounts(pathsWithOutputs, idFromToDatesMaps)
  }

  def calcPersonIdDateMilisCountsAll(
    rootPath: String,
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]],
    outputSuffixes: Seq[String])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val paths = IOSpec.dateCounts(rootPath)

    val pathsWithOutputs = paths.map { case (path, dateColumn, outputColName) =>
      val outputCols = outputSuffixes.map(suffix => outputColName + "_" + suffix)

      (path, dateColumn, outputCols)
    }

    calcPersonIdDateMilisMultiCounts(pathsWithOutputs, idFromToDatesMaps)
  }

  ///////////////////////////
  // Calc for given inputs //
  ///////////////////////////

  def calcPersonIdCounts(
    inputs: Seq[(String, String)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    // run each in parallel
    Future.sequence(
      inputs.map((personIdCount(_: String, _: String)).tupled)
    ).map(groupIntResults)

  def calcPersonIdDateCounts(
    inputs: Seq[(String, String, String)],
    idFromToDatesMap: Map[Int, (Date, Date)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    // run each in parallel
    Future.sequence(
      inputs.map((personIdDateCount(idFromToDatesMap)(_: String, _: String, _: String)).tupled)
    ).map(groupIntResults)

  def calcPersonIdDateMultiCounts(
    inputs: Seq[(String, String, Seq[String])],
    idFromToDatesMaps: Seq[Map[Int, (Date, Date)]])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    // run each in parallel
    Future.sequence(
      inputs.map((personIdDateMultiCounts(idFromToDatesMaps)(_: String, _: String, _: Seq[String])).tupled)
    ).map(multiCounts => groupIntResults(multiCounts.flatten))

  def calcPersonIdDateMilisMultiCounts(
    inputs: Seq[(String, String, Seq[String])],
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    // run each in parallel
    Future.sequence(
      inputs.map((personIdDateMilisMultiCounts(idFromToDatesMaps)(_: String, _: String, _: Seq[String])).tupled)
    ).map(multiCounts => groupIntResults(multiCounts.flatten))

  def calcPersonIdDateMilisConceptMultiCounts(
    inputs: Seq[(String, String, String, Seq[String])],
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
   // run each in parallel
    Future.sequence(
      inputs.map(
        (personIdDateMilisConceptMultiCounts(idFromToDatesMaps)(
        _: String, _: String, _: String, _: Seq[String])
        ).tupled)
    ).map(multiCounts => groupIntResults(multiCounts.flatten))

  ////
  ////

  protected def groupIntResults(
    results: Seq[(String, scala.collection.Map[Int, Int])]
  ) = {
    val (columnNames, grouppedResults) = groupResults(results)
    val intResults = grouppedResults.map { case (personId, personResults) =>
      (personId, personResults.map(_.getOrElse(0)))
    }
    (columnNames, intResults)
  }

  protected def groupResults[T](
    results: Seq[(String, scala.collection.Map[Int, T])]
  ): (Seq[String], Seq[(Int, Seq[Option[T]])]) = {
    val personIds = results.flatMap(_._2.keySet).toSet.toSeq.sorted

    // link person ids with results
    val personResults = personIds.map { personId =>
      (personId, results.map(_._2.get(personId)))
    }

    val columnNames = results.map(_._1)
    (columnNames, personResults)
  }

  private def groupLongCounts(
    counts: Seq[(String, scala.collection.Map[Long, Int])]
  ): (Seq[String], Seq[(Int, Seq[Int])]) = {
    val personIds = counts.flatMap(_._2.keySet).toSet.toSeq.sorted

    // link person ids with counts
    val personCounts = personIds.map { personId =>
      (personId, counts.map(_._2.get(personId).getOrElse(0)))
    }

    val columnNames = counts.map(_._1)
    (columnNames, personCounts.map { case (id, counts) => (id.toInt, counts)})
  }

  private def saveToFile(
    columnNames: Seq[String],
    groupCounts: Seq[(Int, Seq[Int])],
    outputFileName: String
  ) = {
    // file header
    val header = (Seq("person_id") ++ columnNames).mkString(",")

    // export the resulting counts into a file
    val lines = groupCounts.map { case (personId, counts) => (Seq(personId) ++ counts).mkString(",") }.mkString("\n")
    AkkaFileSource.writeStringAsStream(header + "\n" + lines, new java.io.File(outputFileName))
  }

  def personIdMaxDate(
    inputPath: String,
    dateColumnName: String,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateSource = AkkaFileSource.intDateCsvSource(inputPath, personColumnName, dateColumnName)

    personIdDateSource.collect { case Some(x) => x }.via(AkkaFlow.max[Date]).runWith(Sink.head).map { maxDates =>
      logger.info(s"Max person-id date for '${inputPath}' and column '${personColumnName}' done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total dates: ${maxDates.keySet.size}")
      maxDates
    }
  }

  def personIdDateMilisMultiCounts(
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]])(
    inputPath: String,
    dateColumnName: String,
    outputColumnNames: Seq[String],
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    val personIdDateSource = AkkaFileSource.intMilisDateCsvSource(inputPath, personColumnName, dateColumnName)
    val flows = idFromToDatesMaps.map(AkkaFlow.count1X)

    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(flows)

    personIdDateSource.collect { case Some(x) => x }.via(zippedFlow).runWith(Sink.head).map { multiCounts =>
      logger.info(s"Person id counts for '${inputPath}' filtered between ${idFromToDatesMaps.size} date ranges done in ${new Date().getTime - start.getTime} ms.")
      outputColumnNames.zip(multiCounts).map { case (outputColumnName, counts) =>
        logger.info(s"Total counts: ${counts.map(_._2).sum}")
        (outputColumnName, counts)
      }
    }
  }

  def personIdDateMilisConceptMultiCounts(
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]])(
    inputPath: String,
    dateColumnName: String,
    conceptColumnName: String,
    outputColumnNames: Seq[String],
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    val personIdDateConceptSource = AkkaFileSource.int2MilisDateCsvSource(inputPath, personColumnName, dateColumnName, conceptColumnName)
    val flows = idFromToDatesMaps.map(AkkaFlow.count1X)

    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(flows)

    personIdDateConceptSource
      .collect { case (x, Some(y), z) => (x, y, z) }
      .via(AkkaFlow.tuple3To2[Int, Long])
      .via(zippedFlow)
      .runWith(Sink.head)
      .map { multiCounts =>
        logger.info(s"Person id counts for '${inputPath}' filtered between ${idFromToDatesMaps.size} date ranges done in ${new Date().getTime - start.getTime} ms.")
        outputColumnNames.zip(multiCounts).map { case (outputColumnName, counts) =>
        logger.info(s"Total counts: ${counts.map(_._2).sum}")
        (outputColumnName, counts)
      }
    }
  }

  def personIdDateMultiCounts(
    idFromToDatesMaps: Seq[Map[Int, (Date, Date)]])(
    inputPath: String,
    dateColumnName: String,
    outputColumnNames: Seq[String],
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    val personIdDateSource = AkkaFileSource.intDateCsvSource(inputPath, personColumnName, dateColumnName)
    val flows = idFromToDatesMaps.map(AkkaFlow.count1)
    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(flows)

    personIdDateSource
      .collect { case Some(x) => x }
      .via(zippedFlow)
      .runWith(Sink.head)
      .map { multiCounts =>
        logger.info(s"Person id counts for '${inputPath}' filtered between ${idFromToDatesMaps.size} date ranges done in ${new Date().getTime - start.getTime} ms.")
        outputColumnNames.zip(multiCounts).map { case (outputColumnName, counts) =>
        logger.info(s"Total counts: ${counts.map(_._2).sum}")
        (outputColumnName, counts)
      }
    }
  }

  def personIdDateCount(
    idFromToDatesMap: Map[Int, (Date, Date)])(
    inputPath: String,
    dateColumnName: String,
    outputColumnName: String,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateSource = AkkaFileSource.intDateCsvSource(inputPath, personColumnName, dateColumnName)

    personIdDateSource.collect { case Some(x) => x }.via(AkkaFlow.count1(idFromToDatesMap)).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' filtered between dates done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }

  def personIdDateMilisCount(
    inputPath: String,
    idFromToDatesMap: Map[Int, (Long, Long)],
    dateColumnName: String,
    outputColumnName: String,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateSource = AkkaFileSource.intMilisDateCsvSource(inputPath, personColumnName, dateColumnName)

    personIdDateSource.collect { case Some(x) => x }.via(AkkaFlow.count1X(idFromToDatesMap)).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' filtered between dates done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }

  def personIdDateMilisConceptCount(
    inputPath: String,
    idFromToDatesMap: Map[Int, (Long, Long)],
    dateColumnName: String,
    conceptColumnName: String,
    outputColumnName: String,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateConceptSource = AkkaFileSource.int2MilisDateCsvSource(inputPath, personColumnName, dateColumnName, conceptColumnName)

    personIdDateConceptSource.collect { case (x, Some(y), z) => (x, y, z) }.via(AkkaFlow.tuple3To2[Int, Long]).via(AkkaFlow.count1X(idFromToDatesMap)).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' filtered between dates done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }

  private def personIdCount(
    inputPath: String,
    outputColumnName: String,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    val personIdSource = AkkaFileSource.intCsvSource(inputPath, personColumnName)

    personIdSource.via(AkkaFlow.count1).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }
}