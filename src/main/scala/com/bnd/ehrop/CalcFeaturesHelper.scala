package com.bnd.ehrop

import java.util.{Calendar, Date, TimeZone}

import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow, AkkaStreamUtil}
import com.bnd.ehrop.akka.AkkaFileSource.{csvAsSourceWithTransform}
import com.bnd.ehrop.model._
import _root_.akka.stream.Materializer
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}

import sys.process._
import FeatureCalcTypes._
import com.bnd.ehrop.model.TableExt._
import org.apache.commons.io.FileUtils

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import java.io.File

trait CalcFeaturesHelper extends BasicHelper {

  private val milisInYear: Long = 365.toLong * 24 * 60 * 60 * 1000

  def calcAndExportFeatures(
    inputRootPath: String,
    featureSpecs: Seq[TableFeatures],
    dateIntervals: Seq[DayInterval],
    conceptCategories: Seq[ConceptCategory],
    scores: Seq[Score],
    timeZone: TimeZone,
    withTimeLags: Boolean = false,
    outputFileName: String
  )(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val outputRootPath = withBackslash(new File(outputFileName).getParent)

    def tableFile(table: Table) = table.path(inputRootPath)
    def tableFileSorted(table: Table) = table.sortPath(outputRootPath)

    import Table._

    // check mandatory files
    fileExistsOrError(tableFile(person))
    fileExistsOrError(tableFile(visit_occurrence))

    // is a death file provided?
    val hasDeathFile = fileExists(tableFile(death))

    // feature specs count
    val featureSpecsCount = featureSpecs.map(_.extractions.size).sum

    for {
      // sort if needed
      _ <- if (withTimeLags) sortByDateForTableFeatures(inputRootPath, outputRootPath, featureSpecs, timeZone) else Future(())

      // the last visit start dates per person
      lastVisitMilisDates <- personIdMaxMilisDate(
        if (withTimeLags) tableFileSorted(visit_occurrence) else tableFile(visit_occurrence),
        Table.visit_occurrence.visit_start_date.toString,
        withTimeLags,
        timeZone
      ).map(_.toMap)

      // calc death counts in 6 months and turn death counts into 'hasDied' flags
      deadIn6MonthsPersonIds <-
        if (hasDeathFile) {
          val dateRangeIn6MonthsMap = dateIntervalsMilis(lastVisitMilisDates, 0, 180)

          personIdMilisDateCount(
            tableFile(death),
            dateRangeIn6MonthsMap,
            Table.death.death_date.toString,
            "",
            false,
            timeZone
          ).map { case (_, deadCounts) => deadCounts.filter(_._2 > 0).map(_._1).toSet }
        } else {
          logger.warn(s"Death file '${tableFile(death)}' not found. Skipping.")
          Future(Set[Int]())
        }

      // calc features for different the periods
      featureResults <- {
        val dateIntervalsWithLabels = dateIntervals.map { case DayInterval(label, fromDaysShift, toDaysShift) =>
          val range = dateIntervalsMilis(lastVisitMilisDates, fromDaysShift, toDaysShift)
          (range, label)
        }

        calcFeatures(
          if (withTimeLags) outputRootPath else inputRootPath,
          dateIntervalsWithLabels,
          lastVisitMilisDates,
          featureSpecs,
          conceptCategories,
          withTimeLags,
          timeZone
        ).map { features =>
          logger.info(s"Feature generation for ${featureSpecsCount} feature specs, ${dateIntervals.size} date intervals, and ${conceptCategories.size} concept categories finished. Obtained ${features.columnNames.size} features in total.")
          if (scores.nonEmpty) {
            logger.info(s"Note that in addition, ${scores.size} scores will be calculated for ${dateIntervals.size} date intervals.")
          }
          features
        }
      }

      personFeaturesMap = featureResults.personFeatures.toMap

      // person feature strings map
      personFeatureStringsMap = featureResults.personFeatures.map { case (personId, personResults) =>
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
              val yearOfBirth = intValue(Table.person.year_of_birth)(els)
              val monthOfBirth = intValue(Table.person.month_of_birth)(els)
              val dayOfBirth = intValue(Table.person.day_of_birth)(els)
              val genderConcept = intValue(Table.person.gender_concept_id)(els)
              val genderBinarized = genderConcept.map { genderConcept => if (genderConcept == 8532) 1 else 0 }
              val race = intValue(Table.person.race_concept_id)(els)
              val ethnicity = intValue(Table.person.ethnicity_concept_id)(els)

              val birthDate = yearOfBirth match {
                case Some(year) => Some(AkkaFileSource.toCalendar(year, monthOfBirth.getOrElse(1), dayOfBirth.getOrElse(1), timeZone).getTime.getTime)
                case None => dateMilisValue(Table.person.birth_datetime)(els)
              }

              val lastVisitMilisDate = lastVisitMilisDates.get(personId)
              if (lastVisitMilisDate.isEmpty)
                logger.warn(s"No last visit found for the person id ${personId}.")
              val ageAtLastVisit = (lastVisitMilisDate, birthDate).zipped.headOption.map { case (endDate, birthDate) =>
                (endDate - birthDate).toDouble / milisInYear
              }
              val isDeadIn6Months = deadIn6MonthsPersonIds.contains(personId)

              val features = personFeatureStringsMap.get(personId).getOrElse(notFoundValues)

              val scoreValues = scores.flatMap { score =>
                calcIntervalScores(
                  score,
                  dateIntervals,
                  featureSpecs,
                  featureResults.columnNames,
                  personFeaturesMap.get(personId).getOrElse(featureResults.notFoundValues)
                )
              }

              (
                Seq(
                  personId,
                  genderBinarized.getOrElse(""),
                  race.getOrElse(""),
                  ethnicity.getOrElse(""),
                  ageAtLastVisit.getOrElse(""),
                  yearOfBirth.getOrElse(""),
                  monthOfBirth.getOrElse(""),
                  lastVisitMilisDate.getOrElse("")
                ) ++ (
                  if (hasDeathFile) Seq(isDeadIn6Months) else Nil
                ) ++ features ++ scoreValues
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
        val scoreColumnNames =
          dateIntervals.flatMap { dateInterval =>
            scores.map(score => asLowerCaseUnderscore(score.name) + "_" + dateInterval.label)
          }

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
            ) ++ featureResults.columnNames ++ scoreColumnNames
        ).mkString(",")

        logger.info(s"Exporting results to '${outputFileName}.")
        AkkaFileSource.writeLines(Source(List(header)).concat(personOutputSource), outputFileName)
      }
    } yield
      System.exit(0)
  }

  private def asLowerCaseUnderscore(string: String) =
    string.replaceAll("[^\\p{Alnum}]", "_").toLowerCase

  private def calcIntervalScores(
    score: Score,
    dateIntervals: Seq[DayInterval],
    featureSpecs: Seq[TableFeatures],
    columnNames: Seq[String],
    values: Seq[Option[Any]]
  ): Seq[Int] = {
    val dayIntervalCategoryNames = featureSpecs.flatMap { tableFeatures =>
      val tableName = tableFeatures.table.name

      dateIntervals.flatMap { dateInterval =>
        val intervalName = dateInterval.label

        tableFeatures.extractions.map { feature =>
          feature match {
            case x: ConceptCategoryExists[_] =>
              Some((intervalName, x.categoryName))

            case x: ConceptCategoryCount[_] =>
              Some((intervalName, x.categoryName))

            case x: ConceptCategoryIsLastDefined[_] =>
              Some((intervalName, x.categoryName))

            case _ =>
              None
          }
        }
      }
    }

    val intervalCategoryIndecesMap = dayIntervalCategoryNames.zipWithIndex
      .collect { case (Some(names), index) => (names, index) }.groupBy(_._1)
      .map { case (names, values) => (names, values.map(_._2))}

    dateIntervals.map { dateInterval =>
      val intervalName = dateInterval.label

      score.elements.map { element =>
        val sum = element.categoryNames.map { categoryName =>
          intervalCategoryIndecesMap.get((intervalName, categoryName)).map { indeces =>
            indeces.flatMap(values(_)).map(_.asInstanceOf[Int]).sum
          }.getOrElse(0)
        }.sum

        if (sum > 0)
          element.weight
        else
          0
      }.sum
    }
  }

  def calcFeatures(
    rootPath: String,
    idDateRangesWithLabels: Seq[(Map[Int, (Long, Long)], String)],
    idLastVisitDateMap: Map[Int, Long],
    tableFeatures: Seq[TableFeatures],
    conceptCategories: Seq[ConceptCategory],
    withTimeLags: Boolean,
    timeZone: TimeZone = defaultTimeZone
  )(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[FeatureResults] = {

    // create feature executors with data columns
    val categoryNameConceptIdsMap = conceptCategories.map(category => (category.name, category.conceptIds)).toMap
    val executorsAndColumns = tableFeatures.map(featureExecutorsAndColumns(categoryNameConceptIdsMap, idLastVisitDateMap))

    // for each table create input spec with seq executors
    val seqExecsWithInputs = tableFeatures.map(_.table).zip(executorsAndColumns).map { case (table, (executors, dataColumns)) =>
      // group flows based on date filtering and create seq-feature executors
      val outputSpecs = executors.map(_.outputSpecs.map(_.asInstanceOf[FeatureExecutorOutputSpec[Any, Any]]))

      // for each date-range group group flows into a seq flow
      val seqExecutors = idDateRangesWithLabels.map { case (dateMap, rangeLabel) =>
        // zip the flows
        val flows = executors.map(_.flow().asInstanceOf[EHRFlow[Any]])
        val seqFlow = AkkaFlow.filterDate2[Seq[Option[Int]]](dateMap).collect { case Some(x) => x }.via(AkkaStreamUtil.zipNFlows(flows))

        // add date to output columns
        val newOutputSpecs = outputSpecs.map(_.map(spec => spec.copy(outputColumnName = spec.outputColumnName + "_" + rangeLabel)))
        SeqFeatureExecutors(seqFlow, newOutputSpecs)
      }

      // time lag executor(s)
      val timeLagExecs =
        if (withTimeLags) {
          val timeLagExecutor = FeatureExecutorFactory[table.Col](table.name)(TimeLagStats())

          val timeLagSeqFlow = AkkaStreamUtil.zipNFlows(Seq(timeLagExecutor.flow().asInstanceOf[EHRFlow[Any]]))
          val timeLagOutputSpecs = timeLagExecutor.outputSpecs.map(_.asInstanceOf[FeatureExecutorOutputSpec[Any, Any]])
          Seq(SeqFeatureExecutors(timeLagSeqFlow, Seq(timeLagOutputSpecs)))
        } else
          Nil

      // if a sorted file is to be used get its appropriate path
      val path = if (withTimeLags) table.sortPath(rootPath) else table.path(rootPath)

      // input spec
      val input = TableFeatureExecutorInputSpec(
        path,
        table.dateColumn.toString,
        dataColumns
      )

      (seqExecutors ++ timeLagExecs, input)
    }

    calcCustomFeaturesMultiInputs(seqExecsWithInputs, withTimeLags, timeZone)
  }

  def calcCustomFeaturesMultiInputs(
    execsWithInputs: Seq[(Seq[SeqFeatureExecutors[Any, Any]], TableFeatureExecutorInputSpec)],
    dateStoredAsMilis: Boolean,
    timeZone: TimeZone = defaultTimeZone)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[FeatureResults] = {
    // run each in parallel
    for {
      rawResults <- Future.sequence(
        execsWithInputs.map { case (execs, input) => calcCustomFeatures(execs, input, dateStoredAsMilis, timeZone) }
      )
    } yield {
      val flattenedResults = rawResults.flatten
      logger.info(s"Grouping ${flattenedResults.size} features/results.")

      val outputSpecs = flattenedResults.map(_._1)
      val (columnNames, results) = groupResults(outputSpecs, flattenedResults.map(_._2))

      FeatureResults(columnNames, results, outputSpecs.map(_.undefinedValue))
    }
  }

  private def calcCustomFeatures[T, OUT](
    executors: Seq[SeqFeatureExecutors[T, OUT]],
    input: TableFeatureExecutorInputSpec,
    dateStoredAsMilis: Boolean,
    timeZone: TimeZone = defaultTimeZone)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ): Future[Seq[(FeatureExecutorOutputSpec[T, OUT], mutable.Map[Int, OUT])]] = {
    val start = new Date()

    // create a source
    val ehrDataSource = AkkaFileSource.ehrDataCsvSource(input.filePath, input.idColumnName, input.dateColumnName, input.dataColumnNames, dateStoredAsMilis, timeZone)
    // zip the flows
    val zippedFlow = AkkaStreamUtil.zipNFlows(executors.map(_.flows))

    ehrDataSource
      .collect { case (x, Some(y), z) => (x, y, z) } // filter out entries with null dates
      .via(zippedFlow)
      .runWith(Sink.head)
      .map { multiResults =>
        val flattenedResults = multiResults.flatten
        logger.info(s"Processing '${input.filePath}' with ${flattenedResults.size} flows done in ${new Date().getTime - start.getTime} ms.")

        val outSpecs = executors.flatMap(_.outputSpecs)

        (outSpecs, flattenedResults).zipped.flatMap { case (outputSpecs, results) =>
          outputSpecs.map { outputSpec =>
            // post process
            val processedResult = results.flatMap { case (personId, value) => outputSpec.postProcess(personId, value).map((personId, _)) }

            // console out
            logger.info(s" Results for '${outputSpec.outputColumnName}': ${outputSpec.consoleOut(processedResult)}")

            (outputSpec, processedResult)
          }
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
    idDates: Map[Int, Long],
    startShiftDays: Int,
    endShiftDays: Int
  ): Map[Int, (Long, Long)] =
    idDates.map { case (personId, lastVisitDate) =>

      val startCalendar = Calendar.getInstance()
      startCalendar.setTimeInMillis(lastVisitDate)
      startCalendar.add(Calendar.DAY_OF_YEAR, startShiftDays)

      val endCalendar = Calendar.getInstance()
      endCalendar.setTimeInMillis(lastVisitDate)
      endCalendar.add(Calendar.DAY_OF_YEAR, endShiftDays)

      (personId, (startCalendar.getTime.getTime, endCalendar.getTime.getTime))
    }

  private def featureExecutorsAndColumns(
    categoryNameConceptIdsMap: Map[String, Set[Int]],
    personLastVisitDateMap: Map[Int, Long])(
    tableFeatures: TableFeatures
  ): (Seq[FeatureExecutor[_, _]], Seq[String]) = {
    // ref columns
    val refColumns = tableFeatures.extractions.flatMap(_.inputColumns).toSet.toSeq
    val columns = refColumns.map(_.toString)

    // create executors
    val executorFactory = FeatureExecutorFactory[tableFeatures.table.Col](tableFeatures.table.name, refColumns, categoryNameConceptIdsMap, personLastVisitDateMap)
    val executors = tableFeatures.extractions.map(executorFactory.apply)

    (executors, columns)
  }

  protected def groupResults[T](
    outputSpecs: Seq[FeatureExecutorOutputSpec[_, T]],
    results: Seq[scala.collection.Map[Int, T]]
  ): (Seq[String], Seq[(Int, Seq[Option[Any]])]) = {
    val personIds = results.flatMap(_.keySet).toSet.toSeq.sorted

    // link person ids with results
    val personResults = personIds.map { personId =>
      val personResults = results.zip(outputSpecs).map { case (result, outputSpec) =>
        result.get(personId) match {
          case Some(value) => Some(value)
          case None => outputSpec.undefinedValue
        }
      }
      (personId, personResults)
    }

    val columnNames = outputSpecs.map(_.outputColumnName)
    (columnNames, personResults)
  }

  private def personIdMaxMilisDate(
    inputPath: String,
    dateColumnName: String,
    dateStoredAsMilis: Boolean,
    timeZone: TimeZone,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateSource = AkkaFileSource.idMilisDateCsvSource(inputPath, personColumnName, dateColumnName, dateStoredAsMilis, timeZone)

    personIdDateSource.collect { case Some(x) => x }.via(AkkaFlow.max[Long]).runWith(Sink.head).map { maxDates =>
      logger.info(s"Max person-id date for '${inputPath}' and column '${personColumnName}' done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total dates: ${maxDates.keySet.size}")
      maxDates
    }
  }

  private def personIdMilisDateCount(
    inputPath: String,
    idFromToDatesMap: Map[Int, (Long, Long)],
    dateColumnName: String,
    outputColumnName: String,
    dateStoredAsMilis: Boolean,
    timeZone: TimeZone,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val start = new Date()
    val personIdDateSource = AkkaFileSource.idMilisDateCsvSource(inputPath, personColumnName, dateColumnName, dateStoredAsMilis, timeZone)

    personIdDateSource.collect { case Some(x) => x }.via(AkkaFlow.count1X(idFromToDatesMap)).runWith(Sink.head).map { counts =>
      logger.info(s"Person id count for '${inputPath}' filtered between dates done in ${new Date().getTime - start.getTime} ms.")
      logger.info(s"Total counts: ${counts.map(_._2).sum}")
      (outputColumnName, counts)
    }
  }

  def sortByDateForTableFeatures(
    inputPath: String,
    outputPath: String,
    tableFeatures: Seq[TableFeatures],
    timeZone: TimeZone = defaultTimeZone,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) =
    Future.sequence(
      tableFeatures.map { tableFeatures =>
        val extraColumns = tableFeatures.extractions.flatMap(_.inputColumns.map(_.toString)).toSet.toSeq
        val table = tableFeatures.table
        sortByDate(inputPath, outputPath, table, extraColumns, timeZone, personColumnName)
      }
    )

  private def sortByDate(
    inputPath: String,
    outputPath: String,
    table: Table,
    dataColumnNames: Seq[String] = Nil,
    timeZone: TimeZone = defaultTimeZone,
    personColumnName: String = Table.person.person_id.toString)(
    implicit materializer: Materializer, executionContext: ExecutionContext
  ) = {
    val inputFileName = inputPath + table.fileName
    val coreFileName = outputPath + "core-" + table.fileName
    val sortedFileName = outputPath + "sorted-" + table.fileName
    logger.info(s"Sorting of '${inputFileName}' by date started.")

    val start = new Date()

    val lineSource = AkkaFileSource.milisDateEhrDataStringCsvSource(inputFileName, personColumnName, table.dateColumn.toString, dataColumnNames, timeZone)
    AkkaFileSource.writeLines(lineSource, coreFileName).map { _ =>
      logger.info(s"Export of a core data file from '${inputFileName}' finished in ${new Date().getTime - start.getTime} ms.")

      // sort
      logger.info(s"Sorting the core data file '${coreFileName}' by date...")
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