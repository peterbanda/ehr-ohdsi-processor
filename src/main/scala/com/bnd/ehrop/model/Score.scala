package com.bnd.ehrop.model

case class Score(
  name: String,
  elements: Seq[ScoreElement]
)

case class ScoreElement(
  categoryNames: Seq[String],
  weight: Int
)

case class DynamicScore(
  name: String,
  categoryNameGroups: Seq[Seq[String]]
)