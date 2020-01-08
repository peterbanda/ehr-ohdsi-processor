package com.bnd.ehrop

import com.bnd.ehrop.FeatureCalcTypes.{EHRData, EHRFlow, SeqEHRFlow}
import com.bnd.ehrop.akka.{AkkaFlow, DiffStatsAccum}
import com.bnd.ehrop.model._
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.mutable

trait FeatureExecutorFactory[C] {
  def apply(feature: FeatureExtraction[C]): FeatureExecutor[_, _]
}

object FeatureExecutorFactory {
  def apply[C](
    tableName: String,
    refColumns: Seq[C] = Nil,
    categoryNameConceptIdsMap: Map[String, Set[Int]] = Map()
  ): FeatureExecutorFactory[C] =
    new FeatureExecutorFactoryImpl(tableName, refColumns, categoryNameConceptIdsMap)
}

// We use 7 flows/feature generation types:
// - 1. count
// - 2. distinct (concept) count
// - 3. sum
// - 4. last defined concept
// - 5. exists in a concept category
// - 6. count in a concept category
// - 7. is last in a concept category
// - 8. time lags basic stats
private final class FeatureExecutorFactoryImpl[C](
  tableName: String,
  refColumns: Seq[C],
  categoryNameConceptIdsMap: Map[String, Set[Int]]
) extends FeatureExecutorFactory[C] {
  private val columnIndexMap = refColumns.zipWithIndex.toMap

  private val milisInDay = 86400000

  private def columnIndex(col: C): Int =
    columnIndexMap.get(col).getOrElse(throw new IllegalArgumentException(s"Column '${col}' not found."))

  def apply(feature: FeatureExtraction[C]): FeatureExecutor[_, _] = {
    // aux function to return an output column name for an extraction with a single output
    def outputColumnName(extraction: SingleOutFeatureExtraction[C]) =
      tableName + "_" + extraction.outputColumn.name

    feature match {
      // 1. count
      case spec: Count[C] =>
        val flow = () => Flow[EHRData]
          .map { case (x, _, _) => x } // we take only the ids
          .via(AkkaFlow.countAll)

        FeatureExecutor[Int, Int](
          flow,
          postProcess = Some[Int](_),
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 2. distinct (concept) count
      case spec: DistinctCount[C] =>
        val index = columnIndex(spec.column)
        val flow = () => Flow[EHRData]
          .map { case (x, _, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.collectDistinct[Int])

        FeatureExecutor[mutable.Set[Int], Int](
          flow,
          postProcess = (value: mutable.Set[Int]) => Some(value.size),
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 3. sum
      case spec: Sum[C] =>
        val index = columnIndex(spec.column)
        val flow = () => Flow[EHRData]
          .map { case (x, _, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.sum)

        FeatureExecutor[Int, Int](
          flow,
          postProcess = Some[Int](_),
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 4. last defined concept
      case spec: LastDefinedConcept[C] =>
        val index = columnIndex(spec.conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, y, z(index)) }
          .via(AkkaFlow.lastDefined[Long, Int])

        FeatureExecutor[(Long, Int), Int](
          flow,
          postProcess = (value: (Long, Int)) => Some(value._2),
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName(spec),
          undefinedValue = None
        )

      // 5. checks if there is a concept that belongs to a given category
      case spec: ConceptCategoryExists[C] =>
        val ids = categoryNameConceptIdsMap.get(spec.categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${spec.categoryName}' not found."))
        val index = columnIndex(spec.conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, _, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.existsIn(ids))

        FeatureExecutor[Boolean, Int](
          flow,
          postProcess = (value: Boolean) => Some(if (value) 1 else 0),
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 6. counts all the concepts that belongs to a given category
      case spec: ConceptCategoryCount[C] =>
        val ids = categoryNameConceptIdsMap.get(spec.categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${spec.categoryName}' not found."))
        val index = columnIndex(spec.conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, _, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.countIn(ids))

        FeatureExecutor[Int, Int](
          flow,
          postProcess = Some[Int](_),
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 7. checks if the last-defined concept belongs to a given category
      case spec: ConceptCategoryIsLastDefined[C] =>
        val ids = categoryNameConceptIdsMap.get(spec.categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${spec.categoryName}' not found."))
        val index = columnIndex(spec.conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, y, z(index)) }
          .via(AkkaFlow.lastDefined[Long, Int])

        FeatureExecutor[(Long, Int), Int](
          flow,
          postProcess = (tuple: (Long, Int)) => Some(if (ids.contains(tuple._2)) 1 else 0),
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName(spec),
          undefinedValue = Some(0)
        )

      // 8. time lag stats - mean, std, min, max
      case spec: TimeLagStats[C] =>
        val flow = () => Flow[EHRData]
          .map { case (x, y, _) => (x, y.toDouble / milisInDay) }
          .via(AkkaFlow.calcDiffStats)

        def outputSpec[T, Double](
          postProcess: T => Option[Double],
          suffix: String,
          consoleOut: Option[mutable.Map[Int, Double] => String] = None
        ) = {
          val consoleOutX = consoleOut.getOrElse(
            (map: mutable.Map[Int, Double]) => map.map(_._2).size.toString
          )

          FeatureExecutorOutputSpec[T, Double](
            postProcess = postProcess,
            consoleOut = consoleOutX,
            tableName + "_" + spec.outputColumn(suffix).name,
          )
        }

        val outputs = Seq(
          outputSpec(
            (accum: DiffStatsAccum) => AkkaFlow.calcDiffStats(accum).map(_.mean),
            "mean"
          ),

          outputSpec(
            (accum: DiffStatsAccum) => AkkaFlow.calcDiffStats(accum).map(_.std),
            "std"
          ),

          outputSpec(
            (accum: DiffStatsAccum) => AkkaFlow.calcDiffStats(accum).map(_.min),
            "min"
          ),

          outputSpec(
            (accum: DiffStatsAccum) => AkkaFlow.calcDiffStats(accum).map(_.max),
            "max"
          ),

          outputSpec(
            (accum: DiffStatsAccum) => AkkaFlow.calcDiffStats(accum).flatMap(_.mostFreqRelativeDiff),
            "rel_diff_most_freq",
            Some {
              (map: mutable.Map[Int, Double]) =>
                val values = map.map(_._2)
                def count(value: Double) = values.count(_ == value)

                s"${values.size} => -1: ${count(-1)}, -0.5: ${count(-0.5)}, 0: ${count(0)}, 0.5: ${count(0.5)}, 1: ${count(1)}"
            }
          )
        )

        FeatureExecutor[DiffStatsAccum, Double](flow, outputs)
    }
  }
}