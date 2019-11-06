package com.bnd.ehrop

import java.text.{ParseException, ParsePosition, SimpleDateFormat}

import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.bnd.ehrop.AkkaFileSource.csvAsSourceWithTransform
import java.util.{Calendar, Date}

import org.ada.server.akka.AkkaStreamUtil
import com.bnd.ehrop.Table._
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}

trait PersonIdCountHelper {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)

  protected val defaultDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  private def ioSpecs(rootPath: String) = {
    val dataPath = DataPath(rootPath)
    Seq(
      // visit_occurrence
      (dataPath.visit_occurrence, "visit_occurrence_count"),
      // condition_occurrence
      (dataPath.condition_occurrence, "condition_occurrence_count"),
      // observation_period
      (dataPath.observation_period, "observation_period_count"),
      // observation
      (dataPath.observation, "observation_count"),
      // measurement
      (dataPath.measurement, "measurement_count"),
      // procedure_occurrence
      (dataPath.procedure_occurrence, "procedure_occurrence_count"),
      // drug_exposure
      (dataPath.drug_exposure, "drug_exposure_count")
    )
  }

  protected def ioDateSpecs(rootPath: String) = {
    val dataPath = DataPath(rootPath)
    Seq(
      // visit_occurrence
      (dataPath.visit_occurrence, visit_occurrence.visit_end_date.toString, "visit_occurrence_count"), // visit_end_datetime
      // condition_occurrence
      (dataPath.condition_occurrence, condition_occurrence.condition_start_date.toString, "condition_occurrence_count"), // condition_end_datetime
      // observation_period
      (dataPath.observation_period, observation_period.observation_period_end_date.toString, "observation_period_count"),
      // observation
      (dataPath.observation, observation.observation_date.toString, "observation_count"), // observation_datetime
      // measurement
      (dataPath.measurement, measurement.measurement_date.toString, "measurement_count"), // measurement_datetime
      // procedure_occurrence
      (dataPath.procedure_occurrence, procedure_occurrence.procedure_date.toString, "procedure_occurrence_count"), //  procedure_datetime
      // drug_exposure
      (dataPath.drug_exposure, drug_exposure.drug_exposure_start_date.toString, "drug_exposure_count") // exposure_end_datetime
    )
  }

  protected def ioDateConceptSpecs(rootPath: String) = {
    val dataPath = DataPath(rootPath)
    Seq(
      // visit_occurrence
      (
        dataPath.visit_occurrence,
        visit_occurrence.visit_end_date.toString,
        visit_occurrence.visit_concept_id.toString,
        "visit_occurrence"
      ),

      // condition_occurrence
      (
        dataPath.condition_occurrence,
        condition_occurrence.condition_start_date.toString,
        condition_occurrence.condition_concept_id.toString,
        "condition_occurrence"
      ),

      // observation_period
      (
        dataPath.observation_period,
        observation_period.observation_period_end_date.toString,
        observation_period.period_type_concept_id.toString,
        "observation_period"
      ),

      // observation
      (
        dataPath.observation,
        observation.observation_date.toString,
        observation.observation_concept_id.toString,
        "observation"
      ),

      // measurement
      (
        dataPath.measurement,
        measurement.measurement_date.toString,
        measurement.measurement_concept_id.toString,
        "measurement"
      ),

      // procedure_occurrence
      (
        dataPath.procedure_occurrence,
        procedure_occurrence.procedure_date.toString,
        procedure_occurrence.procedure_concept_id.toString,
        "procedure_occurrence"
      ),

      // drug_exposure
      (
        dataPath.drug_exposure,
        drug_exposure.drug_exposure_start_date.toString,
        drug_exposure.drug_concept_id.toString,
        "drug_exposure"
      )
    )
  }

  def calcPersonIdCountsAndOutput(
    rootPath: String)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Unit = {
    val inputs = ioSpecs(rootPath)
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
    val inputs = ioDateSpecs(rootPath)

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
    calcPersonIdCounts(ioSpecs(rootPath))

  def calcPersonIdDateCountsAll(
    rootPath: String,
    idFromToDatesMap: Map[Int, (Date, Date)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    calcPersonIdDateCounts(ioDateSpecs(rootPath), idFromToDatesMap)

  def calcPersonIdDateCountsAll(
    rootPath: String,
    idFromToDatesMaps: Seq[Map[Int, (Date, Date)]],
    outputSuffixes: Seq[String])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val paths = ioDateSpecs(rootPath)

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
    val paths = ioDateSpecs(rootPath)

    val pathsWithOutputs = paths.map { case (path, dateColumn, outputColName) =>
      val outputCols = outputSuffixes.map(suffix => outputColName + "_" + suffix)

      (path, dateColumn, outputCols)
    }

    calcPersonIdDateMilisMultiCounts(pathsWithOutputs, idFromToDatesMaps)
  }

  def calcPersonIdDateMilisConceptCountsAll(
    rootPath: String,
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]],
    outputSuffixes: Seq[String])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val paths = ioDateConceptSpecs(rootPath)

    val pathsWithOutputs = paths.map { case (path, dateColumn, conceptColumn, tableName) =>
      val outputCols = outputSuffixes.map(suffix => tableName + "_count_" + suffix)

      (path, dateColumn, conceptColumn, outputCols)
    }

    calcPersonIdDateMilisConceptMultiCounts(pathsWithOutputs, idFromToDatesMaps)
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
    val personIdDateSource = intDateCsvSource(inputPath, personColumnName, dateColumnName)

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

    val personIdDateSource = intMilisDateCsvSource(inputPath, personColumnName, dateColumnName)
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

    val personIdDateConceptSource = int2MilisDateCsvSource(inputPath, personColumnName, dateColumnName, conceptColumnName)
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

    val personIdDateSource = intDateCsvSource(inputPath, personColumnName, dateColumnName)
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
    val personIdDateSource = intDateCsvSource(inputPath, personColumnName, dateColumnName)

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
    val personIdDateSource = intMilisDateCsvSource(inputPath, personColumnName, dateColumnName)

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
    val personIdDateConceptSource = int2MilisDateCsvSource(inputPath, personColumnName, dateColumnName, conceptColumnName)

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

    val personIdSource = intCsvSource(inputPath, personColumnName)

    personIdSource.via(AkkaFlow.count1).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }

  private def intCsvSource(
    inputPath: String,
    columnName: String
  ): Source[Int, _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = columnIndexMap.get(columnName).get

        def int(els: Array[String]) = els(intColumnIndex).trim

        els => int(els).toDouble.toInt
      }
    )

  private def intMilisDateCsvSource(
    inputPath: String,
    intColumnName: String,
    dateColumnName: String
  ): Source[Option[(Int, Long)], _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = columnIndexMap.get(intColumnName).get
        val dateColumnIndex = columnIndexMap.get(dateColumnName).get

        def int(els: Array[String]) = els(intColumnIndex).trim.toDouble.toInt
        def dateSafe(els: Array[String]): Option[Long] = asDateMilis(els(dateColumnIndex).trim, inputPath)

        els =>
          try {
            dateSafe(els).map((int(els), _))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${intColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  protected def int2MilisDateCsvSource(
    inputPath: String,
    intColumnName1: String,
    dateColumnName: String,
    intColumnName2: String
  ): Source[(Int, Option[Long], Option[Int]), _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex1 = columnIndexMap.get(intColumnName1).get
        val intColumnIndex2 = columnIndexMap.get(intColumnName2).get
        val dateColumnIndex = columnIndexMap.get(dateColumnName).get

        def asInt(string: String) = string.toDouble.toInt
        def asIntOptional(string: String) = if (string.nonEmpty) Some(asInt(string)) else None

        def int1(els: Array[String]) = asInt(els(intColumnIndex1).trim)
        def int2(els: Array[String]) = asIntOptional(els(intColumnIndex2).trim)
        def date(els: Array[String]): Option[Long] = asDateMilis(els(dateColumnIndex).trim, inputPath)

        els =>
          try {
            (int1(els), date(els), int2(els))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${intColumnName1}', '${intColumnName2}', and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  private def intDateCsvSource(
    inputPath: String,
    intColumnName: String,
    dateColumnName: String
  ): Source[Option[(Int, Date)], _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = columnIndexMap.get(intColumnName).get
        val dateColumnIndex = columnIndexMap.get(dateColumnName).get

        def int(els: Array[String]) = els(intColumnIndex).trim.toDouble.toInt
        def dateSafe(els: Array[String]): Option[Date] = asDate(els(dateColumnIndex).trim, inputPath)

        els =>
          try {
            dateSafe(els).map((int(els), _))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${intColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e
          }
      }
    )

  protected def asDateX(
    dateString: String,
    inputPath: String
  ) =
    if (dateString.nonEmpty) {
      val date = try {
        val parsePosition = new ParsePosition(0)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        dateFormat.parse(dateString, parsePosition)
      } catch {
        case e: ParseException =>
          logger.error(s"Cannot parse a date string '${dateString}' for the path '${inputPath}'.")
          throw e

        case e: Exception =>
          logger.error(s"Fatal problem for a date string '${dateString}' and the path '${inputPath}'.")
          throw e
      }
      Some(date)
    } else {
      None
    }

  protected def asDateMilis(
    dateString: String,
    inputPath: String
  ) =
    asCalendar(dateString, inputPath).map(_.getTimeInMillis)

  protected def asDate(
    dateString: String,
    inputPath: String
  ) =
    asCalendar(dateString, inputPath).map(_.getTime)

  protected def asCalendar(
    dateString: String,
    inputPath: String
  ) =
    if (dateString.nonEmpty) {
      val date = try {
        val year = dateString.substring(0, 4).toInt
        val month = dateString.substring(5, 7).toInt
        val day = dateString.substring(8, 10).toInt

        val calendar = Calendar.getInstance()
        calendar.set(Calendar.YEAR, year)
        calendar.set(Calendar.MONTH, month - 1)
        calendar.set(Calendar.DAY_OF_MONTH, day)
        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        calendar
      } catch {
        case e: ParseException =>
          logger.error(s"Cannot parse a date string '${dateString}' for the path '${inputPath}'.", e)
          throw e

        case e: Exception =>
          logger.error(s"Fatal problem for a date string '${dateString}' and the path '${inputPath}'.", e)
          throw e
      }
      Some(date)
    } else {
      None
    }
}