package com.bnd.ehrop.model

import com.bnd.ehrop.model.Table._

object TableExt {

  private val tableDateColumnMap: Map[Table, Enumeration#Value] = Seq(
    // person
    (Table.person, person.birth_datetime),
    // visit_occurrence
    (Table.visit_occurrence, visit_occurrence.visit_start_date), // visit_start_datetime
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

  implicit class Infix(val table: Table) {
    def dateColumn: table.Col = tableDateColumnMap.get(table).get.asInstanceOf[table.Col]

    def path(rootPath: String) = rootPath + table.fileName

    def sortPath(rootPath: String) = rootPath + "sorted-" + table.fileName
  }
}