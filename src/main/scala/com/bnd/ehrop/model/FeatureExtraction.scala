package com.bnd.ehrop.model

trait FeatureExtraction[C] {
  val inputColumns: Seq[C] = Nil
  val outputColumns: Seq[OutputColumn]

  protected def asLowerCaseUnderscore(string: String) =
    string.replaceAll("[^\\p{Alnum}]", "_").toLowerCase
}

trait SingleOutFeatureExtraction[C] extends FeatureExtraction[C] {
  val outputColumn: OutputColumn
  lazy val outputColumns: Seq[OutputColumn] = Seq(outputColumn)
}

case class OutputColumn(
  name: String,
  isNumeric: Boolean
)

// Simple Counts

case class Count[C]() extends SingleOutFeatureExtraction[C] {
  override val outputColumn = OutputColumn("count", true)
}

case class DistinctCount[C](
  column: C
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(column)

  override val outputColumn = OutputColumn(column + "_distinct_count", true)
}

case class Sum[C](
  column: C
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(column)

  override val outputColumn = OutputColumn(column + "_sum", true)
}

// Concept

case class LastDefinedConcept[C](
  conceptColumn: C
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(conceptColumn)

  override val outputColumn = OutputColumn(conceptColumn + "_last_defined", false)
}

// Concept Category

case class ConceptCategoryExists[C](
  conceptColumn: C,
  categoryName: String
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(conceptColumn)

  override val outputColumn = OutputColumn(conceptColumn + "_exists_" + asLowerCaseUnderscore(categoryName), false)
}

case class ConceptCategoryCount[C](
  conceptColumn: C,
  categoryName: String
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(conceptColumn)

  override val outputColumn = OutputColumn(conceptColumn + "_count_" + asLowerCaseUnderscore(categoryName), true)
}

case class ConceptCategoryIsLastDefined[C](
  conceptColumn: C,
  categoryName: String
) extends SingleOutFeatureExtraction[C] {
  override val inputColumns = Seq(conceptColumn)

  override val outputColumn = OutputColumn(conceptColumn + "_last_defined_" + asLowerCaseUnderscore(categoryName), false)
}

// Time lags

case class TimeLagStats[C]() extends FeatureExtraction[C] {
  private def col(suffix: String, isNumeric: Boolean = true) =
    suffix -> OutputColumn("time_lag_" + suffix, isNumeric)

  private val suffixOutputColumns = Seq(
    col("mean"),
    col("std"),
    col("min"),
    col("max"),
    col("rel_diff_most_freq", false),
  )

  override val outputColumns = suffixOutputColumns.map(_._2)

  private val suffixOutputColumnMap = suffixOutputColumns.toMap

  def outputColumn(suffix: String) =
    suffixOutputColumnMap.get(suffix).getOrElse(
      throw new IllegalArgumentException(s"Output column suffix '${suffix}' does not exist for the time lags based features extraction.")
    )
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