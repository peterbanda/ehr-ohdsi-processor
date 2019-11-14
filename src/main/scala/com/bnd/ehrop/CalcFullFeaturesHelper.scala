package com.bnd.ehrop

import java.util.{Calendar, Date}

import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow, AkkaStreamUtil}
import com.bnd.ehrop.akka.AkkaFileSource.{csvAsSourceWithTransform, writeStringAsStream}
import com.bnd.ehrop.model.{DistinctCount, LastDefinedConcept, _}
import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}
import _root_.akka.NotUsed
import FeatureTypes._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

trait CalcFullFeaturesHelper extends PersonIdCountHelper {

  private val milisInYear: Long = 365.toLong * 24 * 60 * 60 * 1000
  private val baseCountFeatures = 3 * 7

  def calcAndExportFeatures(
    inputRootPath: String,
    dayIntervals: Seq[DayInterval],
    outputFileName: Option[String] = None)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val dataPath = DataPath(inputRootPath)

    // check mandatory files
    fileExistsOrError(dataPath.person)
    fileExistsOrError(dataPath.visit_occurrence)

    // is a death file provided?
    val hasDeathFile = fileExists(dataPath.death)

    for {
      // visit end dates per person
      visitEndDates <- personIdMaxDate(
        dataPath.visit_occurrence,
        Table.visit_occurrence.visit_end_date.toString
      ).map(_.toMap)

      // calc death counts in 6 months and turn death counts into 'hasDied' flags
      deadIn6MonthsPersonIds <-
        if (hasDeathFile) {
          val dateRangeIn6MonthsMap = dateIntervalsMilis(visitEndDates, 0, 180)

          personIdDateMilisCount(
            dataPath.death, dateRangeIn6MonthsMap,
            Table.death.death_date.toString, ""
          ).map { case (_, deadCounts) => deadCounts.filter(_._2 > 0).map(_._1).toSet }
        } else {
          logger.warn(s"Death file '${dataPath.death}' not found. Skipping.")
          Future(Set[Int]())
        }

      // calc stats for different the periods
      (countHeaders, personCountsMap) <- {
        val outputLabels = dayIntervals.map(_.label)
        val dateIntervals = dayIntervals.map { case DayInterval(_, fromDaysShift, toDaysShift) =>
          dateIntervalsMilis(visitEndDates, fromDaysShift, toDaysShift)
        }

        calcFeaturesAllPaths(
          inputRootPath,
          dateIntervals,
          outputLabels
        ).map { case (headers, results) =>
          logger.info(s"Date filtering with flows finished.")
          val newResults = results.map { case (personId, rawResults) =>
            val processedResults = rawResults.zipWithIndex.map { case (result, index) =>
              index % 3 match {
                case 0 => result.getOrElse(0)
                case 1 => result.getOrElse(0)
                case 2 => result.getOrElse("")
              }
            }
            (personId, processedResults)
          }
          (headers, newResults.toMap)
        }
      }

      personOutputSource = csvAsSourceWithTransform(dataPath.person,
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

          def dateValue(columnName: Table.person.Value)
            (els: Array[String]) =
            getValue(columnName, els).flatMap(AkkaFileSource.asDate(_, dataPath.person))

          els =>
            try {
              val personId = intValue(Table.person.person_id)(els).getOrElse(throw new RuntimeException(s"Person id missing for the row ${els.mkString(",")}"))
              val birthDate = dateValue(Table.person.birth_datetime)(els)
              val yearOfBirth = intValue(Table.person.year_of_birth)(els)
              val monthOfBirth = intValue(Table.person.month_of_birth)(els)
              val gender = intValue(Table.person.gender_concept_id)(els)
              val race = intValue(Table.person.race_concept_id)(els)
              val ethnicity = intValue(Table.person.ethnicity_concept_id)(els)

              val visitEndDate = visitEndDates.get(personId)
              if (visitEndDate.isEmpty)
                logger.warn(s"No end visit found for the person id ${personId}.")
              val ageAtLastVisit = (visitEndDate, birthDate).zipped.headOption.map { case (endDate, birthDate) =>
                (endDate.getTime - birthDate.getTime).toDouble / milisInYear
              }
              val isDeadIn6Months = deadIn6MonthsPersonIds.contains(personId)

              val counts = personCountsMap.get(personId).getOrElse(Seq.fill(baseCountFeatures * dayIntervals.size)(0))

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
                ) ++ counts
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
            ) ++ countHeaders
        ).mkString(",")

        val outputFile = outputFileName.getOrElse(inputRootPath + "features.csv")
        logger.info(s"Exporting results to '${outputFile}.")
        AkkaFileSource.writeLines(Source(List(header)).concat(personOutputSource), outputFile)
      }
    } yield
      System.exit(0)
  }

  def calcFeaturesAllPaths(
    rootPath: String,
    idFromToDatesMaps: Seq[Map[Int, (Long, Long)]],
    outputSuffixes: Seq[String],
    conceptGroups: Seq[ConceptGroup] = Nil
  )(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[(Seq[String], Seq[(Int, Seq[Option[Int]])])] = {
    val paths = IOSpec.dateConceptOuts(rootPath)

    val pathsWithOutputs = paths.flatMap { case (path, dateColumn, conceptColumn, outputColName) =>
      if (fileExists(path)) {
        val outputCols = IOSpec.outputColumns(outputColName, Some(conceptColumn), outputSuffixes)
        Some((path, dateColumn, conceptColumn, outputCols))
      } else {
        logger.warn(s"File '${path}' does not exist. Skipping.")
        None
      }
    }

    // We use 3 flows/feature generation types:
    // - 1. counts
    // - 2. distinct counts
    // - 3. last defined concepts
    // - 4. exists group concepts (several possible)
    val flows = () => {
      idFromToDatesMaps.map { dateMap =>
        val countFlow: EHRFlow[Int] = AkkaFlow.countAll[Long, Seq[Option[Int]]]
        val countDistinct: EHRFlow[mutable.Set[Int]] = Flow[EHRData].collect { case (x, y, Seq(Some(z))) => (x, z)}.via(AkkaFlow.collectDistinct[Int])
        val lastConceptFlow: EHRFlow[(Long, Int)] = Flow[EHRData].map { case (x, y, z) => (x, y, z.head) }.via(AkkaFlow.lastDefined[Long, Int])
//        val existConcepFlows: Seq[PersonFlow[Boolean]] = conceptGroups.map { conceptSet =>
//          Flow[PersonData].collect { case (x, y, Some(z)) => (x, z)}.via(AkkaFlow.existsIn(conceptSet.ids))
//        }

        val sameFilterFlows = Seq(countFlow, countDistinct, lastConceptFlow)

        // zip the flows
        AkkaFlow.filterDate2[Seq[Option[Int]]](dateMap).collect { case Some(x) => x }.via(AkkaStreamUtil.zipNFlows(sameFilterFlows)).asInstanceOf[SeqEHRFlow[Any]]
      }
    }

    val postProcess = idFromToDatesMaps.flatMap { _ =>
      Seq(
        (value: Any) => value.asInstanceOf[Int],
        (value: Any) => value.asInstanceOf[mutable.Set[Int]].size,
        (value: Any) => value.asInstanceOf[(Long, Int)]._2
      )
    }

    val consoleOuts = idFromToDatesMaps.flatMap { _ =>
      Seq(
        (map: mutable.Map[Int, Int]) =>  map.map(_._2).sum.toString,
        (map: mutable.Map[Int, Int]) =>  map.map(_._2).sum.toString,
        (map: mutable.Map[Int, Int]) =>  map.size.toString
      )
    }

    calcCustomFeaturesMultiInputs[Any](pathsWithOutputs, flows, postProcess, consoleOuts)
  }

  def calcCustomFeaturesMultiInputs[T](
    inputs: Seq[(String, String, String, Seq[String])],
    flows: () => Seq[SeqEHRFlow[T]],
    postProcess: Seq[T => Int],
    consoleOuts: Seq[mutable.Map[Int, Int] => String] = Nil)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[(Seq[String], Seq[(Int, Seq[Option[Int]])])] =
    // run each in parallel
    Future.sequence(
      inputs.map(
        (calcCustomFeatures[T, Int](flows(), postProcess, consoleOuts)(
          _: String, _: String, _: String, _: Seq[String])
          ).tupled
      )
    ).map(multiCounts => groupResults(multiCounts.flatten))

  private def calcCustomFeatures[T, OUT](
    flows: Seq[SeqEHRFlow[T]],
    postProcess: Seq[T => OUT],
    consoleOuts: Seq[mutable.Map[Int, OUT] => String] = Nil)(
    inputPath: String,
    dateColumnName: String,
    conceptColumnName: String,
    outputColumnNames: Seq[String],
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()

    // create a source
    val personDateDataSource = AkkaFileSource.idMilisDateDataCsvSource(inputPath, personColumnName, dateColumnName, Seq(conceptColumnName))
    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(flows)

    personDateDataSource
      .collect { case (x, Some(y), z) => (x, y, z) }
      .via(zippedFlow)
      .runWith(Sink.head)
      .map { multiResults =>
        val flattenedResults = multiResults.flatten
        logger.info(s"Processing '${inputPath}' with ${flattenedResults.size} flows done in ${new Date().getTime - start.getTime} ms.")

        val processedResults =
          (postProcess, flattenedResults).zipped.map { case (post, results) =>
            results.map { case (personId, value) => (personId, post(value)) }
          }

        if (consoleOuts.nonEmpty) {
          (outputColumnNames, processedResults, consoleOuts).zipped.map { case  (outputColumnName, results, consoleOut) =>
            logger.info(s" Results for '${outputColumnName}': ${consoleOut(results)}")
            (outputColumnName, results)
          }
        } else
          outputColumnNames.zip(processedResults)
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

  private def featureExecutorsAndColumns[T <: Table](
    tableFeatures: TableFeatures[T]
  ): (Seq[FeatureExecutor[_]], Seq[String]) = {
    // ref columns
    val refColumns = tableFeatures.extractions.flatMap(_.columns).toSet.toSeq.sorted
    val columns = refColumns.map(_.toString)

    // create executors
    val executorFactory = new FeatureExecutorFactory[tableFeatures.table.Col](refColumns)
    val executors = tableFeatures.extractions.map(executorFactory.apply)

    (executors, columns)
  }
}

object FeatureTypes {
  // id, date, and additional data
  type EHRData = (Int, Long, Seq[Option[Int]])
  type EHRFlow[T] = Flow[EHRData, mutable.Map[Int, T], NotUsed]
  type SeqEHRFlow[T] = Flow[EHRData, Seq[mutable.Map[Int, T]], NotUsed]
}

case class FeatureExecutor[T](
  flow: EHRFlow[T],
  postProcess: T => Int,
  consoleOut: mutable.Map[Int, Int] => String
)

class FeatureExecutorFactory[C](refColumns: Seq[C]) {

  private val columnIndexMap = refColumns.zipWithIndex.toMap

  private def columnIndex(col: C): Int =
    columnIndexMap.get(col).getOrElse(throw new IllegalArgumentException(s"Column '${col}' not found."))

  def apply(feature: FeatureExtraction[C]): FeatureExecutor[_] =
    feature match {
      // count
      case Count() =>
        FeatureExecutor[Int](
          AkkaFlow.countAll[Long, Seq[Option[Int]]],
          identity[Int],
          (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString
        )

      // distinct count
      case DistinctCount() =>
        val flow = Flow[EHRData]
          .collect { case (x, y, Seq(Some(z))) => (x, z) }
          .via(AkkaFlow.collectDistinct[Int])

        FeatureExecutor[mutable.Set[Int]](,
          flow,
          (value: mutable.Set[Int]) => value.size,
          (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString
        )

      // last defined concept
      case LastDefinedConcept(conceptColumn) =>
        val index = columnIndex(conceptColumn)
        val flow = Flow[EHRData]
          .map { case (x, y, z) => (x, y, z(index)) }
          .via(AkkaFlow.lastDefined[Long, Int])

        FeatureExecutor[(Long, Int)](
          flow,
          (value: (Long, Int)) => value._2,
          (map: mutable.Map[Int, Int]) => map.size.toString
        )

      // exist concept in group
      case ExistConceptInGroup(conceptColumn, ids, _) =>
        val index = columnIndex(conceptColumn)
        val flow = Flow[EHRData]
          .map { case (x, y, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.existsIn(ids))

        FeatureExecutor[Boolean](
          flow,
          (value: Boolean) => if (value) 1 else 0,
          (map: mutable.Map[Int, Int]) => map.size.toString
        )
    }
}