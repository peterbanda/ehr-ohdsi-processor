package com.bnd.ehrop

import com.bnd.ehrop.FeatureCalcTypes.{EHRData, EHRFlow, SeqEHRFlow}
import com.bnd.ehrop.model._

import scala.collection.mutable

case class FeatureExecutor[T, O](
  flow: () => EHRFlow[T],
  outputSpecs: Seq[FeatureExecutorOutputSpec[T, O]]
)

case class SeqFeatureExecutors[T, O](
  flows: SeqEHRFlow[T],
  outputSpecs: Seq[Seq[FeatureExecutorOutputSpec[T, O]]]
)

case class FeatureExecutorOutputSpec[T, O](
  postProcess: T => Option[O],
  consoleOut: mutable.Map[Int, O] => String,
  outputColumnName: String,
  undefinedValue: Option[O] = None,
)

case class TableFeatureExecutorInputSpec(
  filePath: String,
  dateColumnName: String,
  dataColumnNames: Seq[String],
  idColumnName: String = Table.person.person_id.toString
)

object FeatureExecutor {
  def apply[T, O](
    flow: () => EHRFlow[T],
    postProcess: T => Option[O],
    consoleOut: mutable.Map[Int, O] => String,
    outputColumnName: String,
    undefinedValue: Option[O] = None,
  ):FeatureExecutor[T, O] = FeatureExecutor(
    flow,
    Seq(FeatureExecutorOutputSpec[T, O](
      postProcess,
      consoleOut,
      outputColumnName,
      undefinedValue
    ))
  )
}
