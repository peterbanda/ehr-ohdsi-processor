package com.bnd.ehrop

object Table {

  object person extends Enumeration {
    val year_of_birth,gender_source_value,ethnicity_concept_id,provider_id,race_source_concept_id,person_id,
    person_source_value,month_of_birth,gender_source_concept_id,ethnicity_source_concept_id,care_site_id,
    day_of_birth,ethnicity_source_value,location_id,race_concept_id,gender_concept_id,birth_datetime,race_source_value = Value
  }

  object visit_occurrence extends Enumeration {
    val visit_source_value,discharge_to_source_value,visit_end_datetime,visit_concept_id,person_id,
    admitting_source_value,care_site_id,visit_source_concept_id,visit_occurrence_id,visit_start_datetime,
    visit_end_date,admitting_source_concept_id,preceding_visit_occurrence_id,provider_id,visit_start_date,
    discharge_to_concept_id,visit_type_concept_id = Value
  }

  object condition_occurrence extends Enumeration {
    val provider_id,visit_detail_id,condition_start_datetime,condition_end_datetime,condition_source_concept_id,person_id,
    condition_status_source_value,condition_end_date,condition_start_date,condition_status_concept_id,condition_type_concept_id,
    condition_concept_id,stop_reason,condition_source_value,condition_occurrence_id,visit_occurrence_id = Value
  }

  object observation extends Enumeration {
    val observation_type_concept_id,provider_id,visit_detail_id,value_as_concept_id,observation_date,person_id,
    value_as_number,observation_source_concept_id,value_as_string,unit_source_value,observation_concept_id,qualifier_source_value,
    observation_id,observation_source_value,observation_source_concept_i,unit_concept_id,observation_datetime,
    qualifier_concept_id,visit_occurrence_id = Value
  }

  object observation_period extends Enumeration {
    val period_type_concept_id,observation_period_end_date,person_id,observation_period_id,observation_period_start_date = Value
  }

  object measurement extends Enumeration {
    val value_source_value,measurement_id,measurement_datetime,measurement_type_concept_id,provider_id,operator_concept_id,
    measurement_date,person_id,value_as_number,value_as_concept_id,measurement_concept_id,range_low,measurement_source_concept_id,
    unit_source_value,measurement_time,unit_concept_id,measurement_source_value,range_high,visit_detail_id,visit_occurrence_id = Value
  }

  object procedure_occurrence extends Enumeration {
    val procedure_datetime,provider_id,visit_detail_id,quantity,person_id,procedure_date,procedure_type_concept_id,
    procedure_source_concept_id,modifier_concept_id,procedure_concept_id,procedure_source_value,modifier_source_value,
    procedure_occurrence_id,visit_occurrence_id = Value
  }

  object drug_exposure extends Enumeration {
    val person_id,drug_exposure_start_date,refills,drug_source_value,drug_exposure_end_date,route_concept_id,
    quantity,lot_number,days_supply,sig,drug_type_concept_id,drug_source_concept_id,provider_id,route_source_value,
    drug_exposure_id,dose_unit_source_value,drug_exposure_start_datetime,drug_exposure_end_datetime,visit_detail_id,
    verbatim_end_date,stop_reason,drug_concept_id,visit_occurrence_id = Value
  }

  object death extends Enumeration {
    val person_id,death_date,death_datetime,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id = Value
  }
}