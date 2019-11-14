package com.bnd.ehrop.model

import com.bnd.ehrop.model.Table._
import TableFeatures.withDefault

object TableFeaturesSpecs {

  val apply = Seq(
    // visit_occurrence
    withDefault(visit_occurrence)(
      DistinctCount(visit_occurrence.visit_concept_id),
      LastDefinedConcept(visit_occurrence.visit_concept_id)
    ),

    // condition_occurrence
    withDefault(condition_occurrence)(
      DistinctCount(condition_occurrence.condition_concept_id),
      LastDefinedConcept(condition_occurrence.condition_concept_id)
    ),

    // observation_period
    withDefault(observation_period)(
      DistinctCount(observation_period.period_type_concept_id),
      LastDefinedConcept(observation_period.period_type_concept_id)
    ),

    // observation
    withDefault(observation)(
      DistinctCount(observation.observation_concept_id),
      LastDefinedConcept(observation.observation_concept_id)
    ),

    // measurement
    withDefault(measurement)(
      DistinctCount(measurement.measurement_concept_id),
      LastDefinedConcept(measurement.measurement_concept_id)
    ),

    // procedure_occurrence
    withDefault(procedure_occurrence)(
      DistinctCount(procedure_occurrence.procedure_concept_id),
      LastDefinedConcept(procedure_occurrence.procedure_concept_id)
    ),

    // drug_exposure
    withDefault(drug_exposure)(
      DistinctCount(drug_exposure.drug_concept_id),
      LastDefinedConcept(drug_exposure.drug_concept_id)
    )
  ).asInstanceOf[Seq[TableFeatures[Table]]]
}

object Implicits {

  private val map: Map[Table, Enumeration#Value] = Seq(
    // person
    (Table.person, person.birth_datetime),
    // visit_occurrence
    (Table.visit_occurrence, visit_occurrence.visit_end_date), // visit_end_datetime
    // condition_occurrence
    (Table.condition_occurrence, condition_occurrence.condition_start_date), // condition_end_datetime
    // observation_period
    (Table.observation_period, observation_period.observation_period_end_date),
    // observation
    (Table.observation, observation.observation_date), // observation_datetime
    // measurement
    (Table.measurement, measurement.measurement_date), // measurement_datetime
    // procedure_occurrence
    (Table.procedure_occurrence, procedure_occurrence.procedure_date), //  procedure_datetime
    // drug_exposure
    (Table.drug_exposure, drug_exposure.drug_exposure_start_date) // exposure_end_datetime
  ).toMap

  implicit class TableExt(val table: Table) {
    def dateColumn: table.Col = map.get(table).get.asInstanceOf[table.Col]

    def path(rootPath: String) = rootPath + table.name + ".csv"
  }
}