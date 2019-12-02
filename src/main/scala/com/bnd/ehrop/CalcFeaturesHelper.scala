package com.bnd.ehrop

import java.util.{Calendar, Date}

import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow, AkkaStreamUtil}
import com.bnd.ehrop.akka.AkkaFileSource.{csvAsSourceWithTransform, writeStringAsStream}
import com.bnd.ehrop.model.{DistinctCount, FeatureResults, LastDefinedConcept, _}
import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}
import _root_.akka.NotUsed
import FeatureTypes._
import com.bnd.ehrop.model.TableExt._
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait CalcFeaturesHelper {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)
  private val milisInYear: Long = 365.toLong * 24 * 60 * 60 * 1000

  def calcAndExportFeatures(
    inputRootPath: String,
    featureSpecs: Seq[TableFeatures],
    dayIntervals: Seq[DayInterval],
    outputFileName: Option[String] = None)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val tableFile = TableFileName(inputRootPath)

    import Table._

    // check mandatory files
    fileExistsOrError(tableFile(person))
    fileExistsOrError(tableFile(visit_occurrence))

    // is a death file provided?
    val hasDeathFile = fileExists(tableFile(death))

    // features count
    val featuresCount = featureSpecs.map(_.extractions.size).sum

    for {
      // visit end dates per person
      visitEndDates <- personIdMaxDate(
        tableFile(visit_occurrence),
        Table.visit_occurrence.visit_end_date.toString
      ).map(_.toMap)

      // calc death counts in 6 months and turn death counts into 'hasDied' flags
      deadIn6MonthsPersonIds <-
        if (hasDeathFile) {
          val dateRangeIn6MonthsMap = dateIntervalsMilis(visitEndDates, 0, 180)

          personIdDateMilisCount(
            tableFile(death), dateRangeIn6MonthsMap,
            Table.death.death_date.toString, ""
          ).map { case (_, deadCounts) => deadCounts.filter(_._2 > 0).map(_._1).toSet }
        } else {
          logger.warn(s"Death file '${tableFile(death)}' not found. Skipping.")
          Future(Set[Int]())
        }

      // calc features for different the periods
      featureResults <- {
        val dateIntervalsWithLabels = dayIntervals.map { case DayInterval(label, fromDaysShift, toDaysShift) =>
          val range = dateIntervalsMilis(visitEndDates, fromDaysShift, toDaysShift)
          (range, label)
        }

        calcFeatures(
          inputRootPath,
          dateIntervalsWithLabels,
          featureSpecs
        ).map { features =>
          logger.info(s"Feature generation for ${featuresCount} features and ${dayIntervals.size} date intervals finished.")
          features
        }
      }

      // person-features map
      personFeaturesMap = featureResults.personFeatures.map { case (personId, personResults) =>
        (personId, personResults.map(_.getOrElse("")))
      }.toMap

      // not-found values to report if a person not found
      notFoundValues = featureResults.notFoundValues.map(_.map(_.toString).getOrElse(""))

      personOutputSource = csvAsSourceWithTransform(tableFile(person),
        header => {
          val columnIndexMap = header.zipWithIndex.toMap

          def getValue(
            columnName: Table.person.Value,
            els: Array[String]
          ) =
            els(columnIndexMap.get(columnName.toString).get).trim match {
              case "" => None
              case x: String => Some(x)
            }

          def intValue(columnName: Table.person.Value)
            (els: Array[String]) =
            getValue(columnName, els).map(_.toDouble.toInt)

          def dateMilisValue(columnName: Table.person.Value)
            (els: Array[String]) =
            getValue(columnName, els).flatMap(AkkaFileSource.asDateMilis(_, tableFile(person)))

          els =>
            try {
              val personId = intValue(Table.person.person_id)(els).getOrElse(throw new RuntimeException(s"Person id missing for the row ${els.mkString(",")}"))
              val birthDate = dateMilisValue(Table.person.birth_datetime)(els)
              val yearOfBirth = intValue(Table.person.year_of_birth)(els)
              val monthOfBirth = intValue(Table.person.month_of_birth)(els)
              val gender = intValue(Table.person.gender_concept_id)(els)
              val race = intValue(Table.person.race_concept_id)(els)
              val ethnicity = intValue(Table.person.ethnicity_concept_id)(els)

              val visitEndDate = visitEndDates.get(personId)
              if (visitEndDate.isEmpty)
                logger.warn(s"No end visit found for the person id ${personId}.")
              val ageAtLastVisit = (visitEndDate, birthDate).zipped.headOption.map { case (endDate, birthDate) =>
                (endDate.getTime - birthDate).toDouble / milisInYear
              }
              val isDeadIn6Months = deadIn6MonthsPersonIds.contains(personId)

              val features = personFeaturesMap.get(personId).getOrElse(notFoundValues)

              (
                Seq(
                  personId,
                  gender.getOrElse(""),
                  race.getOrElse(""),
                  ethnicity.getOrElse(""),
                  ageAtLastVisit.getOrElse(""),
                  yearOfBirth.getOrElse(""),
                  monthOfBirth.getOrElse(""),
                  visitEndDate.map(_.getTime).getOrElse("")
                ) ++ (
                  if (hasDeathFile) Seq(isDeadIn6Months) else Nil
                ) ++ features
              ).mkString(",")
            } catch {
              case e: Exception =>
                logger.error(s"Problem found while processing a person table/csv at line: ${els.mkString(", ")}", e)
                throw e
            }
        }
      )

      // exporting
      _ <- {
        val header = (
          Seq(
            "person_id",
            "gender",
            "race",
            "ethnicity",
            "age_at_last_visit",
            "year_of_birth",
            "month_of_birth",
            "visit_end_date"
          ) ++ (
            if (hasDeathFile) Seq("died_6_months_after_last_visit") else Nil
            ) ++ featureResults.columnNames
        ).mkString(",")

        val outputFile = outputFileName.getOrElse(inputRootPath + "features.csv")
        logger.info(s"Exporting results to '${outputFile}.")
        AkkaFileSource.writeLines(Source(List(header)).concat(personOutputSource), outputFile)
      }
    } yield
      System.exit(0)
  }

  def calcFeatures(
    rootPath: String,
    idDateRangesWithLabels: Seq[(Map[Int, (Long, Long)], String)],
    tableFeatures: Seq[TableFeatures])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[FeatureResults] = {
    // create feature executors with data columns
    val executorsAndColumns = tableFeatures.map(featureExecutorsAndColumns)

    // for each table create input spec with seq executors
    val seqExecsWithInputs = tableFeatures.map(_.table).zip(executorsAndColumns).map { case (table, (executors, dataColumns)) =>
      // group flows based on date filtering and create seq-feature executors
      val extras = executors.map(_.extra.asInstanceOf[FeatureExecutorExtra[Any]])

      val seqExecutors = idDateRangesWithLabels.map { case (dateMap, rangeLabel) =>
        // zip the flows
        val flows = executors.map(_.flow().asInstanceOf[EHRFlow[Any]])
        val seqFlow = AkkaFlow.filterDate2[Seq[Option[Int]]](dateMap).collect { case Some(x) => x }.via(AkkaStreamUtil.zipNFlows(flows))

        // add date to output columns
        val newExtras = extras.map(extra => extra.copy(outputColumnName = extra.outputColumnName + "_" + rangeLabel))
        SeqFeatureExecutors(seqFlow, newExtras)
      }

      // input spec
      val input = TableFeatureExecutorInputSpec(
        table.path(rootPath),
        table.dateColumn.toString,
        dataColumns
      )

      (seqExecutors, input)
    }

    calcCustomFeaturesMultiInputs[Any](seqExecsWithInputs)
  }

  def calcCustomFeaturesMultiInputs[T](
    execsWithInputs: Seq[(Seq[SeqFeatureExecutors[T]], TableFeatureExecutorInputSpec)])(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[FeatureResults] =
    // run each in parallel
    Future.sequence(
      execsWithInputs.map { case (execs, input) => calcCustomFeatures[T, Int](execs)(input) }
    ).map { multiCounts =>
      val undefinedValues = execsWithInputs.flatMap(_._1.flatMap(_.extras.map(_.undefinedValue)))
      val (columnNames, results) = groupResults(multiCounts.flatten, undefinedValues)

      FeatureResults(columnNames, results, undefinedValues)
    }

  private def calcCustomFeatures[T, OUT](
    executors: Seq[SeqFeatureExecutors[T]])(
    input: TableFeatureExecutorInputSpec)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    // create a source
    val ehrDataSource = AkkaFileSource.ehrDataCsvSource(input.filePath, input.idColumnName, input.dateColumnName, input.dataColumnNames)
    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(executors.map(_.flows))

    ehrDataSource
      .collect { case (x, Some(y), z) => (x, y, z) }
      .via(zippedFlow)
      .runWith(Sink.head)
      .map { multiResults =>
        val flattenedResults = multiResults.flatten
        logger.info(s"Processing '${input.filePath}' with ${flattenedResults.size} flows done in ${new Date().getTime - start.getTime} ms.")

        val extras = executors.flatMap(_.extras)

        // post process
        val processedResults =
          (extras.map(_.postProcess), flattenedResults).zipped.map { case (post, results) =>
            results.map { case (personId, value) => (personId, post(value)) }
          }

        // console outs
        (extras.map(_.outputColumnName), processedResults, extras.map(_.consoleOut)).zipped.map { case  (outputColumnName, results, consoleOut) =>
          logger.info(s" Results for '${outputColumnName}': ${consoleOut(results)}")
          (outputColumnName, results)
        }
      }
  }

  protected def fileExists(name: String) =
    new java.io.File(name).exists

  protected def fileExistsOrError(name: String) =
    if (!fileExists(name)) {
      val message = s"The input path '${name}' does not exist. Exiting."
      logger.error(message)
      System.exit(1)
    }

  private def dateIntervalsMilis(
    idDates: Map[Int, Date],
    startShiftDays: Int,
    endShiftDays: Int
  ): Map[Int, (Long, Long)] =
    idDates.map { case (personId, lastVisitDate) =>

      val startCalendar = Calendar.getInstance()
      startCalendar.setTime(lastVisitDate)
      startCalendar.add(Calendar.DAY_OF_YEAR, startShiftDays)

      val endCalendar = Calendar.getInstance()
      endCalendar.setTime(lastVisitDate)
      endCalendar.add(Calendar.DAY_OF_YEAR, endShiftDays)

      (personId, (startCalendar.getTime.getTime, endCalendar.getTime.getTime))
    }

  private def featureExecutorsAndColumns(
    tableFeatures: TableFeatures
  ): (Seq[FeatureExecutor[_]], Seq[String]) = {
    // ref columns
    val refColumns = tableFeatures.extractions.flatMap(_.columns).toSet.toSeq
    val columns = refColumns.map(_.toString)

    // create executors
    val executorFactory = new FeatureExecutorFactory[tableFeatures.table.Col](tableFeatures.table.name, refColumns)
    val executors = tableFeatures.extractions.map(executorFactory.apply)

    (executors, columns)
  }

  protected def groupResults[T](
    results: Seq[(String, scala.collection.Map[Int, T])],
    notFoundValues: Seq[Option[T]]
  ): (Seq[String], Seq[(Int, Seq[Option[T]])]) = {
    val personIds = results.flatMap(_._2.keySet).toSet.toSeq.sorted

    // link person ids with results
    val personResults = personIds.map { personId =>
      val personResults = results.zip(notFoundValues).map { case ((_, result), notFoundValue) =>
        result.get(personId) match {
          case Some(value) => Some(value)
          case None => notFoundValue
        }
      }
      (personId, personResults)
    }

    val columnNames = results.map(_._1)
    (columnNames, personResults)
  }

  private def personIdMaxDate(
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

  private def personIdDateMilisCount(
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
}

object FeatureTypes {
  // id, date, and additional data
  type EHRData = (Int, Long, Seq[Option[Int]])
  type EHRFlow[T] = Flow[EHRData, mutable.Map[Int, T], NotUsed]
  type SeqEHRFlow[T] = Flow[EHRData, Seq[mutable.Map[Int, T]], NotUsed]
}

case class FeatureExecutor[T](
  flow: () => EHRFlow[T],
  postProcess: T => Int,
  consoleOut: mutable.Map[Int, Int] => String,
  outputColumnName: String,
  undefinedValue: Option[Int] = None,
) {
  def extra = FeatureExecutorExtra(
    postProcess,
    consoleOut,
    outputColumnName,
    undefinedValue
  )
}

case class SeqFeatureExecutors[T](
  flows: SeqEHRFlow[T],
  extras: Seq[FeatureExecutorExtra[T]]
)

case class FeatureExecutorExtra[T](
  postProcess: T => Int,
  consoleOut: mutable.Map[Int, Int] => String,
  outputColumnName: String,
  undefinedValue: Option[Int] = None,
)

case class TableFeatureExecutorInputSpec(
  filePath: String,
  dateColumnName: String,
  dataColumnNames: Seq[String],
  idColumnName: String = Table.person.person_id.toString
)

// We use 4 flows/feature generation types:
// - 1. counts
// - 2. distinct counts
// - 3. last defined concepts
// - 4. exists group concepts (several possible)
final class FeatureExecutorFactory[C](tableName: String, refColumns: Seq[C]) {

  private val columnIndexMap = refColumns.zipWithIndex.toMap

  private def columnIndex(col: C): Int =
    columnIndexMap.get(col).getOrElse(throw new IllegalArgumentException(s"Column '${col}' not found."))

  def apply(feature: FeatureExtraction[C]): FeatureExecutor[_] = {
    val outputColumnName = tableName + "_" + feature.label
    feature match {
      // count
      case Count() =>
        FeatureExecutor[Int](
          flow = () => AkkaFlow.countAll[Long, Seq[Option[Int]]],
          postProcess = identity[Int],
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )

      // distinct count
      case DistinctCount(conceptColumn) =>
        val index = columnIndex(conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.collectDistinct[Int])

        FeatureExecutor[mutable.Set[Int]](
          flow,
          postProcess = (value: mutable.Set[Int]) => value.size,
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )

      // last defined concept
      case LastDefinedConcept(conceptColumn) =>
        val index = columnIndex(conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, y, z(index)) }
          .via(AkkaFlow.lastDefined[Long, Int])

        FeatureExecutor[(Long, Int)](
          flow,
          postProcess = (value: (Long, Int)) => value._2,
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName,
          undefinedValue = None
        )

      // exist concept in group
      case ExistConceptInGroup(conceptColumn, ids, _) =>
        val index = columnIndex(conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.existsIn(ids))

        FeatureExecutor[Boolean](
          flow,
          postProcess = (value: Boolean) => if (value) 1 else 0,
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )
    }
  }
}