package com.bnd.ehrop.model

import com.bnd.ehrop.model.Table._
import TableFeatures.withDefault

object TableFeaturesSpecs {

  val apply = Seq(
    // visit_occurrence
    withDefault(visit_occurrence)(
      LastDefinedConcept(visit_occurrence.visit_concept_id)
    ),

    // condition_occurrence
    withDefault(condition_occurrence)(
      LastDefinedConcept(condition_occurrence.condition_concept_id)
    ),

    // observation_period
    withDefault(observation_period)(
      LastDefinedConcept(observation_period.period_type_concept_id)
    ),

    // observation
    withDefault(observation)(
      LastDefinedConcept(observation.observation_concept_id)
    ),

    // measurement
    withDefault(measurement)(
      LastDefinedConcept(measurement.measurement_concept_id)
    ),

    // procedure_occurrence
    withDefault(procedure_occurrence)(
      LastDefinedConcept(procedure_occurrence.procedure_concept_id)
    ),

    // drug_exposure
    withDefault(drug_exposure)(
      LastDefinedConcept(drug_exposure.drug_concept_id)
    )
  )
}
