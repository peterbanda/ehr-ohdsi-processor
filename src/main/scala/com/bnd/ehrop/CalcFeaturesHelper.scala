package com.bnd.ehrop

import java.util.{Calendar, Date, TimeZone}

import com.bnd.ehrop.akka.{AkkaFileSource, AkkaFlow, AkkaStreamUtil}
import com.bnd.ehrop.akka.AkkaFileSource.csvAsSourceWithTransform
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

import com.bnd.ehrop.model.Table.person

trait CalcFeaturesHelper extends BasicHelper {

  private val milisInYear: Long = 365.toLong * 24 * 60 * 60 * 1000

  def calcAndExportFeatures(
    inputRootPath: String,
    featureSpecs: Seq[TableFeatures],
    dateIntervals: Seq[DayInterval],
    conceptCategories: Seq[ConceptCategory],
    scores: Seq[Score],
    dynamicScores: Seq[DynamicScore],
    timeZone: TimeZone,
    withTimeLags: Boolean = false,
    dynamicScoreWeightsOutputFileName: Option[String],
    dynamicScoreWeightsInputFileName: Option[String],
    outputFileName: String)(
    implicit materializer: Materializer,
    executionContext: ExecutionContext
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

      // calc death counts in 0-6 months and turn them into 'hasDied' flags
      deadBefore6MonthsPersonIds <-
        if (hasDeathFile) {
          val dateRangeBefore6MonthsMap = dateIntervalsMilis(lastVisitMilisDates, 0, 180)

          personIdMilisDateCount(
            tableFile(death),
            dateRangeBefore6MonthsMap,
            Table.death.death_date.toString,
            "",
            false,
            timeZone
          ).map { case (_, deadCounts) => deadCounts.filter(_._2 > 0).map(_._1).toSet }
        } else {
          logger.warn(s"Death file '${tableFile(death)}' not found. Skipping generation of 0-6 months 'dead' flags.")
          Future(Set[Int]())
        }

      // calc death counts in 6-inf months and turn them into 'hasDied' flags
      deadAfter6MonthsPersonIds <-
        if (hasDeathFile) {
          val dateRangeAfter6MonthsMap = dateIntervalsMilis(lastVisitMilisDates, 180, 1000)

          personIdMilisDateCount(
            tableFile(death),
            dateRangeAfter6MonthsMap,
            Table.death.death_date.toString,
            "",
            false,
            timeZone
          ).map { case (_, deadCounts) => deadCounts.filter(_._2 > 0).map(_._1).toSet }
        } else {
          logger.warn(s"Death file '${tableFile(death)}' not found. Skipping generation of 6-inf months 'dead' flags.")
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
          if (scores.nonEmpty || dynamicScores.nonEmpty) {
            logger.info(s"Note that in addition ${scores.size} scores and ${dynamicScores.size} dynamic scores will be calculated for ${dateIntervals.size} date intervals.")
          }
          features
        }
      }

      personFeaturesMap = featureResults.personFeatures.toMap

      // person feature strings map
      personFeatureStringsMap = featureResults.personFeatures.map { case (personId, personResults) =>
        (personId, personResults.map(_.getOrElse("")))
      }.toMap

      // not-found feature values to report if a person not found
      notFoundFeatureValues = featureResults.notFoundValues.map(_.map(_.toString).getOrElse(""))

      // date interval-category name -> indeces map
      intervalCategoryIndecesMap = getIntervalCategoryFeatureIndecesMap(dateIntervals, featureSpecs, withTimeLags)

      // calc or import the weights for given dynamic scores
      dynamicScoreWeights <-
        if (dynamicScoreWeightsInputFileName.isDefined) {
          logger.info(s"Parsing and importing the dynamic score weights from '${dynamicScoreWeightsInputFileName.get}'...")
          importDynamicScoreWeights(dynamicScoreWeightsInputFileName.get, dynamicScores)

        } else if (hasDeathFile) {
          logger.info(s"Calculating weights for ${dynamicScores.size} dynamic scores...")

          if (dynamicScores.nonEmpty)
            calcDynamicScoreWeights(
              intervalCategoryIndecesMap,
              inputRootPath,
              dynamicScores,
              dateIntervals,
              featureResults.personFeatures.toMap,
              lastVisitMilisDates,
              deadBefore6MonthsPersonIds,
              timeZone
            )
          else
            Future(Nil)
        } else {
          logger.warn(s"Cannot calculate dynamic score weights because the death file '${tableFile(death)}' is not available.")
          Future(Nil)
        }

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

              val deadFlag =
                if (deadBefore6MonthsPersonIds.contains(personId))
                  1
                else if (deadAfter6MonthsPersonIds.contains(personId))
                  2
                else
                  0 // healthy (didn't die)

              val features = personFeatureStringsMap.get(personId).getOrElse(notFoundFeatureValues)

              // calc the scores
              val scoreValues = scores.flatMap { score =>
                calcIntervalScores(
                  intervalCategoryIndecesMap)(
                  score,
                  dateIntervals,
                  personFeaturesMap.get(personId).getOrElse(featureResults.notFoundValues)
                )
              }

              // calc the dynamic scores
              val dynamicScoreValues =
                if (dynamicScoreWeights.nonEmpty) {
                  if (ageAtLastVisit.isDefined) {
                    dynamicScores.zip(dynamicScoreWeights).flatMap { case (score, intervalAgeBinnedWeights) =>
                      calcIntervalDynamicScores(
                        intervalCategoryIndecesMap)(
                        score,
                        ageAtLastVisit.get,
                        intervalAgeBinnedWeights,
                        dateIntervals,
                        personFeaturesMap.get(personId).getOrElse(featureResults.notFoundValues)
                      )
                    }
                  } else
                     // fill with zeroes (or none?)
                    Seq.fill(dynamicScores.size * dateIntervals.size)(0d)
                } else
                  Nil

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
                  if (hasDeathFile) Seq(deadFlag) else Nil
                ) ++ features ++ scoreValues ++ dynamicScoreValues
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
          scores.flatMap(score =>
            dateIntervals.map { dateInterval => asLowerCaseUnderscore(score.name) + "_" + dateInterval.label }
          )

        val dynamicScoreColumnNames =
          if (dynamicScoreWeights.nonEmpty)
            dynamicScores.flatMap(score =>
              dateIntervals.map { dateInterval => asLowerCaseUnderscore(score.name) + "_" + dateInterval.label }
            )
          else
            Nil

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
            if (hasDeathFile) Seq("died_after_last_visit") else Nil
            ) ++ featureResults.columnNames ++ scoreColumnNames ++ dynamicScoreColumnNames
          ).mkString(",")

        logger.info(s"Exporting results to '${outputFileName}.")
        AkkaFileSource.writeLines(Source(List(header)).concat(personOutputSource), outputFileName)
      }

      // if an output dynamic score weights file name provided -> export
      _ <- if (dynamicScoreWeightsOutputFileName.isDefined) {
        logger.info(s"Exporting dynamic score weights to '${dynamicScoreWeightsOutputFileName.get}.")

        val lines = dynamicScores.zip(dynamicScoreWeights).flatMap { case (dynScore, intervalAgeBinnedWeights) =>
          val intervalWeightsSorted = intervalAgeBinnedWeights.toSeq.sortBy(_._1)
          intervalWeightsSorted.flatMap { case (intervalName, ageBinnedWeights) =>
            ageBinnedWeights.zipWithIndex.map { case (weights, ageBin) =>
              val els = Seq(dynScore.name, intervalName, ageBin) ++ weights
              els.mkString(",")
            }
          }
        }

        AkkaFileSource.writeLines(Source(lines.toList), dynamicScoreWeightsOutputFileName.get)
      } else Future(())
    } yield
      System.exit(0)
  }

  private def calcIntervalScores(
    intervalCategoryIndecesMap: Map[(String, String), Seq[Int]])(
    score: Score,
    dateIntervals: Seq[DayInterval],
    values: Seq[Option[Any]]
  ): Seq[Int] =
    dateIntervals.map { dateInterval =>
      val intervalName = dateInterval.label

      score.elements.map { element =>
        val sum = element.categoryNames.map { categoryName =>
          intervalCategoryIndecesMap.get((intervalName, categoryName)).map { indeces =>
            indeces.flatMap(values(_)).map(_.asInstanceOf[Int]).sum
          }.getOrElse(0)
        }.sum

        if (sum > 0) element.weight else 0
      }.sum
    }

  private def calcIntervalDynamicScores(
    intervalCategoryIndecesMap: Map[(String, String), Seq[Int]])(
    score: DynamicScore,
    age: Double,
    intervalAgeBinnedWeights: Map[String, Seq[Seq[Double]]],
    dateIntervals: Seq[DayInterval],
    values: Seq[Option[Any]]
  ): Seq[Double] = {
    val ageBin = ageToBin(age)

    dateIntervals.map { dateInterval =>
      val intervalName = dateInterval.label
      val ageBinnedWeights = intervalAgeBinnedWeights.get(intervalName).getOrElse(throw new IllegalArgumentException(s"No weights found for the date interval '${intervalName}'."))

      val weights = ageBinnedWeights(ageBin)

      score.categoryNameGroups.zip(weights).map { case (categoryNames, weight) =>
        val sum = categoryNames.map { categoryName =>
          intervalCategoryIndecesMap.get((intervalName, categoryName)).map { indeces =>
            indeces.flatMap(values(_)).map(_.asInstanceOf[Int]).sum
          }.getOrElse(0)
        }.sum

        if (sum > 0) weight else 0d
      }.sum
    }
  }

  private def importDynamicScoreWeights(
    importFileName: String,
    dynamicScores: Seq[DynamicScore])(
    implicit materializer: Materializer,
    executionContext: ExecutionContext
  ): Future[Seq[Map[String, Seq[Seq[Double]]]]] = {
    for {
      scoreNameWeights <- AkkaFileSource.fileSource(importFileName, "\n", true).runWith(Sink.seq).map { lines =>
        lines.map { line =>
          val els = line.split(",", -1).map(_.trim)

          val scoreName = els(0)
          val intervalName = els(1)
          val ageBin = els(2).toInt

          val weights = els.drop(3).map(_.toDouble)

          (scoreName, (intervalName, ageBin, weights.toSeq))
        }
      }
    } yield {
      val scoreNameWeightsMap = scoreNameWeights
        .groupBy(_._1)
        .map { case (scoreName, values) => (scoreName, values.map(_._2)) }

      dynamicScores.map { score =>
        val intervalAgeBinWeights = scoreNameWeightsMap.get(score.name).getOrElse(throw new IllegalArgumentException(s"The weights for score ${score.name} not found in "))

        intervalAgeBinWeights
          .groupBy(_._1)
          .map { case (intervalName, values) =>
            val ageBinWeightsMap = values.map { case (_, ageBin, weights) => (ageBin, weights) }.toMap

            val weightsSeq = for (ageBin <- 0 until ageBinsNum) yield {
              ageBinWeightsMap.get(ageBin).getOrElse(throw new IllegalArgumentException(s"Age bin with an index ${ageBin} not found for dybnamic score '${score.name}' and date interval '${intervalName}'."))
            }
            (intervalName, weightsSeq)
          }
      }
    }
  }

  private def calcDynamicScoreWeights(
    intervalCategoryIndecesMap: Map[(String, String), Seq[Int]],
    inputRootPath: String,
    scores: Seq[DynamicScore],
    dateIntervals: Seq[DayInterval],
    personFeatureValues: Map[Int, Seq[Option[Any]]],
    lastVisitMilisDates: Map[Int, Long],
    deadIn6MonthsPersonIds: Set[Int],
    timeZone: TimeZone)(
    implicit materializer: Materializer,
    executionContext: ExecutionContext
  ): Future[Seq[Map[String, Seq[Seq[Double]]]]] = {
    val inputPath = person.path(inputRootPath)

    for {
      personIdAgePairs <- personIdAges(inputPath, lastVisitMilisDates, timeZone)
    } yield {
      val isDeadAgeFeatureValues = personIdAgePairs
        .collect { case (personId, Some(age)) => (personId, age) }
        .flatMap { case (personId, age) =>
          val isDeadIn6Months = deadIn6MonthsPersonIds.contains(personId)

          personFeatureValues.get(personId).map { features =>
            (isDeadIn6Months, age, features)
          }
        }

      scores.map { score =>
        dateIntervals.map { dateInterval =>
          val intervalName = dateInterval.label

          val weights = calcAgeBinnedDynamicIntervalScoreWeights(
            intervalCategoryIndecesMap,
            intervalName)(
            score,
            isDeadAgeFeatureValues
          )
          (intervalName, weights)
        }.toMap
      }
    }
  }

  private def personIdAges(
    inputPath: String,
    lastVisitMilisDates: Map[Int, Long],
    timeZone: TimeZone)(
    implicit materializer: Materializer
  ) = {
    val personIdAgeSource = csvAsSourceWithTransform(inputPath,
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
          getValue(columnName, els).flatMap(AkkaFileSource.asDateMilis(_, inputPath))

        els =>
          try {
            val personId = intValue(Table.person.person_id)(els).getOrElse(throw new RuntimeException(s"Person id missing for the row ${els.mkString(",")}"))

            val yearOfBirth = intValue(Table.person.year_of_birth)(els)
            val monthOfBirth = intValue(Table.person.month_of_birth)(els)
            val dayOfBirth = intValue(Table.person.day_of_birth)(els)

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

            (personId, ageAtLastVisit)
          } catch {
            case e: Exception =>
              logger.error(s"Problem found while processing a person table/csv at line: ${els.mkString(", ")}", e)
              throw e
          }
      }
    )

    personIdAgeSource.runWith(Sink.seq)
  }

  private def calcAgeBinnedDynamicIntervalScoreWeights(
    intervalCategoryIndecesMap: Map[(String, String), Seq[Int]],
    intervalName: String)(
    score: DynamicScore,
    personFeatureValues: Seq[(Boolean, Double, Seq[Option[Any]])] // the boolean indicates whether a person died, Double is the age
  ): Seq[Seq[Double]] = {
    val diedFeatureValues = personFeatureValues.filter(_._1).map { case (_, age, featureValues) => (age, featureValues) }
    val healthyFeatureValues = personFeatureValues.filter(!_._1).map { case (_, age, featureValues) => (age, featureValues) }

    val diedAgeBinCategoryGroupRatios = calcAgeBinCategoryGroupRatios(
      intervalCategoryIndecesMap,
      intervalName)(
      score,
      diedFeatureValues
    )

    val healthyAgeBinCategoryGroupRatios = calcAgeBinCategoryGroupRatios(
      intervalCategoryIndecesMap,
      intervalName)(
      score,
      healthyFeatureValues
    )

    for (ageBin <- 0 until ageBinsNum) yield {
      val diedRatios = diedAgeBinCategoryGroupRatios.get(ageBin).getOrElse(Seq.fill(score.categoryNameGroups.size)(0d))
      val healthyRatios = healthyAgeBinCategoryGroupRatios.get(ageBin).getOrElse(Seq.fill(score.categoryNameGroups.size)(0d))

      diedRatios.zip(healthyRatios).map { case (diedRatio, healthyRatio) =>
        diedRatio - healthyRatio
      }
    }
  }

  private val ageBinsNum = 3

  private def ageToBin(age: Double) =
    age match {
      case x if x < 60 => 0
      case x if x >= 60 && x <= 80 => 1
      case x if x > 80 => 2
    }

  private def calcAgeBinCategoryGroupRatios(
    intervalCategoryIndecesMap: Map[(String, String), Seq[Int]],
    intervalName: String)(
    score: DynamicScore,
    agePersonFeatureValues: Seq[(Double, Seq[Option[Any]])]
  ): Map[Int, Seq[Double]] = {
    val ageBinCategoryGroupOccurrenceFlags: Seq[(Int, Seq[Int])] = agePersonFeatureValues.map { case (age, values) =>

      val flags = score.categoryNameGroups.map { categoryNames =>
        val sum = categoryNames.map { categoryName =>
          intervalCategoryIndecesMap.get((intervalName, categoryName)).map { indeces =>
            indeces.flatMap(values(_)).map(_.asInstanceOf[Int]).sum
          }.getOrElse(0)
        }.sum

        if (sum > 0) 1 else 0
      }

      (ageToBin(age), flags)
    }

    ageBinCategoryGroupOccurrenceFlags.groupBy(_._1).map { case (ageBin, values) =>
      val ratios = values.map(_._2).transpose.map { scores =>
        scores.sum.toDouble / scores.size
      }

      (ageBin, ratios)
    }
  }

  private def getIntervalCategoryFeatureIndecesMap(
    dateIntervals: Seq[DayInterval],
    featureSpecs: Seq[TableFeatures],
    withTimeLags: Boolean
  ): Map[(String, String), Seq[Int]] = {
    val dayIntervalCategoryNames = featureSpecs.flatMap { tableFeatures =>

      val part1 = dateIntervals.flatMap { dateInterval =>
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

      val part2 = if (withTimeLags) Seq.fill(TimeLagStats().outputColumns.size)(None) else Nil

      part1 ++ part2
    }

    dayIntervalCategoryNames.zipWithIndex
      .collect { case (Some(names), index) => (names, index) }.groupBy(_._1)
      .map { case (names, values) => (names, values.map(_._2))}
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