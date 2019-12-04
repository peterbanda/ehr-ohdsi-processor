package com.bnd.ehrop

import com.bnd.ehrop.FeatureCalcTypes.{EHRData, EHRFlow, SeqEHRFlow}
import com.bnd.ehrop.akka.AkkaFlow
import com.bnd.ehrop.model._
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.mutable

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

// We use 6 flows/feature generation types:
// - 1. count
// - 2. distinct (concept) count
// - 3. last defined concept
// - 4. exists in a concept category
// - 5. count in a concept category
// - 6. is last in a concept category
final class FeatureExecutorFactory[C](
  tableName: String,
  refColumns: Seq[C],
  categoryNameConceptIdsMap: Map[String, Set[Int]]
) {
  private val columnIndexMap = refColumns.zipWithIndex.toMap

  private def columnIndex(col: C): Int =
    columnIndexMap.get(col).getOrElse(throw new IllegalArgumentException(s"Column '${col}' not found."))

  def apply(feature: FeatureExtraction[C]): FeatureExecutor[_] = {
    val outputColumnName = tableName + "_" + feature.label
    feature match {
      // 1. count
      case Count() =>
        FeatureExecutor[Int](
          flow = () => AkkaFlow.countAll[Long, Seq[Option[Int]]],
          postProcess = identity[Int],
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )

      // 2. distinct (concept) count
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

      // 3. last defined concept
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

      // 4. checks if there is a concept that belongs to a given category
      case ConceptCategoryExists(conceptColumn, categoryName) =>
        val ids = categoryNameConceptIdsMap.get(categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${categoryName}' not found."))
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

      // 5. counts all the concepts that belongs to a given category
      case ConceptCategoryCount(conceptColumn, categoryName) =>
        val ids = categoryNameConceptIdsMap.get(categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${categoryName}' not found."))
        val index = columnIndex(conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, z(index)) }
          .collect { case (x, Some(z)) => (x, z) }
          .via(AkkaFlow.countIn(ids))

        FeatureExecutor[Int](
          flow,
          postProcess = identity[Int],
          consoleOut = (map: mutable.Map[Int, Int]) => map.map(_._2).sum.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )

      // 6. checks if the last-defined concept belongs to a given category
      case ConceptCategoryIsLastDefined(conceptColumn, categoryName) =>
        val ids = categoryNameConceptIdsMap.get(categoryName).getOrElse(throw new IllegalArgumentException(s"Concept category '${categoryName}' not found."))
        val index = columnIndex(conceptColumn)
        val flow = () => Flow[EHRData]
          .map { case (x, y, z) => (x, y, z(index)) }
          .via(AkkaFlow.lastDefined[Long, Int])

        FeatureExecutor[(Long, Int)](
          flow,
          postProcess = { case (_: Long, conceptId: Int) => if (ids.contains(conceptId)) 1 else 0 },
          consoleOut = (map: mutable.Map[Int, Int]) => map.size.toString,
          outputColumnName,
          undefinedValue = Some(0)
        )
    }
  }
}