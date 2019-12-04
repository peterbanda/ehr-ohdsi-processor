package com.bnd.ehrop.model

trait FeatureExtraction[C] {
  val label: String
  val columns: Seq[C] = Nil
  val isNumeric: Boolean

  protected def asLowerCaseUnderscore(string: String) =
    string.replaceAll("[^\\p{Alnum}]", "_").toLowerCase
}

// Simple Counts

case class Count[C]() extends FeatureExtraction[C] {
  override val label = "count"
  override val isNumeric = true
}

case class DistinctCount[C](
  conceptColumn: C
) extends FeatureExtraction[C] {
  override val label = "count_distinct"
  override val columns = Seq(conceptColumn)
  override val isNumeric = true
}

// Concept

case class LastDefinedConcept[C](
  conceptColumn: C
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_last_defined"
  override val columns = Seq(conceptColumn)
  override val isNumeric = false
}

// Concept Category

case class ConceptCategoryExists[C](
  conceptColumn: C,
  categoryName: String
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_exists_" + asLowerCaseUnderscore(categoryName)
  override val columns = Seq(conceptColumn)
  override val isNumeric = false
}

case class ConceptCategoryCount[C](
  conceptColumn: C,
  categoryName: String
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_count_" + asLowerCaseUnderscore(categoryName)
  override val columns = Seq(conceptColumn)
  override val isNumeric = true
}

case class ConceptCategoryIsLastDefined[C](
  conceptColumn: C,
  categoryName: String
) extends FeatureExtraction[C] {
  override val label = conceptColumn + "_last_defined_" + asLowerCaseUnderscore(categoryName)
  override val columns = Seq(conceptColumn)
  override val isNumeric = false
}

trait TableFeatures {
  val table: Table
  val extractions: Seq[FeatureExtraction[table.Col]]
}

private abstract class TableFeaturesImpl(val table: Table) extends TableFeatures

object TableFeatures {

  def apply(
    _table: Table)(
    _extractions: FeatureExtraction[_table.Col]*
  ): TableFeatures =
    new TableFeaturesImpl(_table) {
      // ugly that we have to cast...
      override val extractions = _extractions.map(_.asInstanceOf[FeatureExtraction[table.Col]])
    }

  def withDefault(
    table: Table)(
    extractions: FeatureExtraction[table.Col]*
  ): TableFeatures = apply(table)(
    (Seq(Count[table.Col]()) ++ extractions.toSeq) :_*
  )
}