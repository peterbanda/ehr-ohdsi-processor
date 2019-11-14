package com.bnd.ehrop

case class DataPath(rootPath: String) {
  val person = rootPath + "person.csv"
  val visit_occurrence = rootPath + "visit_occurrence.csv"
  val observation_period = rootPath + "observation_period.csv"
  val observation = rootPath + "observation.csv"
  val condition_occurrence = rootPath + "condition_occurrence.csv"
  val procedure_occurrence = rootPath + "procedure_occurrence.csv"
  val measurement = rootPath + "measurement.csv"
  val drug_exposure = rootPath + "drug_exposure.csv"
  val death = rootPath + "death.csv"
}