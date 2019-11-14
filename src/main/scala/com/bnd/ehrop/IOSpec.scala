package com.bnd.ehrop

import com.bnd.ehrop.model.Table._

@Deprecated
object IOSpec {

  def dateConceptOuts(rootPath: String) = {
    val dataPath = DataPath(rootPath)
    Seq(
      // visit_occurrence
      (
        dataPath.visit_occurrence,
        visit_occurrence.visit_end_date.toString,
        visit_occurrence.visit_concept_id.toString,
        "visit_occurrence"
      ),

      // condition_occurrence
      (
        dataPath.condition_occurrence,
        condition_occurrence.condition_start_date.toString,
        condition_occurrence.condition_concept_id.toString,
        "condition_occurrence"
      ),

      // observation_period
      (
        dataPath.observation_period,
        observation_period.observation_period_end_date.toString,
        observation_period.period_type_concept_id.toString,
        "observation_period"
      ),

      // observation
      (
        dataPath.observation,
        observation.observation_date.toString,
        observation.observation_concept_id.toString,
        "observation"
      ),

      // measurement
      (
        dataPath.measurement,
        measurement.measurement_date.toString,
        measurement.measurement_concept_id.toString,
        "measurement"
      ),

      // procedure_occurrence
      (
        dataPath.procedure_occurrence,
        procedure_occurrence.procedure_date.toString,
        procedure_occurrence.procedure_concept_id.toString,
        "procedure_occurrence"
      ),

      // drug_exposure
      (
        dataPath.drug_exposure,
        drug_exposure.drug_exposure_start_date.toString,
        drug_exposure.drug_concept_id.toString,
        "drug_exposure"
      )
    )
  }

  //  private val flowLabels = Seq("count", "count_distinct", "")

  def outputColumns(
    outputColumnName: String,
    conceptColumnName: Option[String],
    outputSuffixes: Seq[String]
  ) =
    outputSuffixes.flatMap(suffix =>
      Seq(
        outputColumnName + "_count_" + suffix,
        outputColumnName + "_count_distinct_" + suffix
      ) ++ (
        if (conceptColumnName.isDefined)
          Seq(outputColumnName + "_" + conceptColumnName.get + "_last_defined_" + suffix)
        else
          Nil
        )
    )
}

case class ConceptGroup(
  label: String,
  ids: Set[Int]
)