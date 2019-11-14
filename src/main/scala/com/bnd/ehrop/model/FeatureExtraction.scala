package com.bnd.ehrop.model

trait FeatureExtraction[C] {
  val label: String
  val columns: Seq[C] = Nil
}

case class Count[C]() extends FeatureExtraction[C] {
  override val label = "count"
}

case class DistinctCount[C](
  conceptColumn: C
) extends FeatureExtraction[C] {
  override val label = "count_distinct"
  override val columns = Seq(conceptColumn)
}

case class LastDefinedConcept[C](
  conceptColumn: C
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_last_defined"
  override val columns = Seq(conceptColumn)
}

case class ExistConceptInGroup[C](
  conceptColumn: C,
  ids: Set[Int],
  groupName: String
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_exists_" + groupName
  override val columns = Seq(conceptColumn)
}

trait TableFeatures[T <: Table] {
  val table: T
  val extractions: Seq[FeatureExtraction[table.Col]]
}

private abstract class TableFeaturesImpl[T <: Table](val table: T) extends TableFeatures[T]

object TableFeatures {

  def apply[T <: Table](
    _table: T)(
    _extractions: FeatureExtraction[_table.Col]*
  ): TableFeatures[T] =
    new TableFeaturesImpl[T](_table) {
      // ugly that we have to cast...
      override val extractions = _extractions.map(_.asInstanceOf[FeatureExtraction[table.Col]])
    }

  def withDefault[T <: Table](
    table: T)(
    extractions: FeatureExtraction[table.Col]*
  ): TableFeatures[T] = apply[T](table)(
    (Seq(Count[table.Col]()) ++ extractions.toSeq) :_*
  )
}