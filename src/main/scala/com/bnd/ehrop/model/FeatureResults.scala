package com.bnd.ehrop.model

case class FeatureResults(
  columnNames: Seq[String],
  personFeatures: Seq[(Int, Seq[Option[Int]])],
  notFoundValues: Seq[Option[Int]]
)
