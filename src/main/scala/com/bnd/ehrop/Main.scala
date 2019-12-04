package com.bnd.ehrop

import com.bnd.ehrop.akka.AkkaFileSource
import _root_.akka.actor.ActorSystem
import _root_.akka.stream.ActorMaterializer

object Main extends App with AppExt {

  val mode = get("mode", args)

  mode.map {
    _ match {
      case "std" => StandardizeApp.run(args)
      case "features" => CalcFeaturesApp.run(args)
      case _ =>
        val message = s"The mode option '${mode.get}' not recognized. Exiting."
        logger.error(message)
        System.exit(1)
    }
  }.getOrElse {
    val message = s"The mode option '-mode=' not specified. Defaulting to 'features'."
    logger.warn(message)
    CalcFeaturesApp.run(args)
  }
}

object StandardizeApp extends Standardize
object CalcFeaturesApp extends CalcFeatures

object ElixhauserCategoriesRead extends App {

  private implicit val system = ActorSystem()
  private implicit val executor = system.dispatcher
  private implicit val materializer = ActorMaterializer()

  private val fileName = "/home/peter/Data/ehr_dream_challenge/elixhauser_presence.csv"

  val stringSource = AkkaFileSource.csvAsSource(fileName, ";").map { els =>
    val categoryName = els(0).trim
    val ids = els(1).trim

    s"""  {
        name: "${categoryName}",
        conceptIds: [${ids}]
      },
    """
  }

  AkkaFileSource.writeLines(stringSource, fileName + ".json").map ( _ => System.exit(0) )
}

object ElixhauserCategories extends App {

  val pairs: Seq[(String, Seq[String])] = Seq(
    ("Congestive Heart Failure",                Seq("condition_occurrence")),
    ("Caridiac Arrhythmia",                     Seq("condition_occurrence","observation")),
    ("Valvular Disease",                        Seq("condition_occurrence","observation")),
    ("Pulmonary Circulation Disorders",         Seq("condition_occurrence")),
    ("Peripheral Vascular Disorders",           Seq("condition_occurrence","observation")),
    ("Hypertension Uncomlicated",               Seq("condition_occurrence")),
    ("Hypertension comlicated",                 Seq("condition_occurrence")),
    ("Paralysis",                               Seq("condition_occurrence")),
    ("Other Neurological Disorders",            Seq("condition_occurrence")),
    ("Chronic Pulmonary Disease",               Seq("condition_occurrence")),
    ("Diabetes Uncomplicated",                  Seq("measurement","condition_occurrence","observation")),
    ("Diabetes Complicated",                    Seq("measurement","condition_occurrence")),
    ("Hypothyroidism",                          Seq("condition_occurrence")),
    ("Renal Failure",                           Seq("condition_occurrence","procedure_occurrence","observation")),
    ("Liver Disease",                           Seq("condition_occurrence","observation")),
    ("Peptic Ulcer Disease excluding bleeding", Seq("condition_occurrence")),
    ("AIDS/HIV",                                Seq("measurement","condition_occurrence")),
    ("Lymphoma",                                Seq("condition_occurrence")),
    ("Metastatic Cancer",                       Seq("condition_occurrence")),
    ("Solid Tumor without Metastasis",          Seq("condition_occurrence")),
    ("Rheumatoid Arthsitis/collagen",           Seq("condition_occurrence")),
    ("Coagulopathy",                            Seq("condition_occurrence")),
    ("Obesity",                                 Seq("measurement","condition_occurrence")),
    ("Weight Loss",                             Seq("measurement","condition_occurrence")),
    ("Fluid and Ecletrolyte Disorders",         Seq("condition_occurrence")),
    ("Blood Loss Anemia",                       Seq("condition_occurrence")),
    ("Deficiency Anemia",                       Seq("condition_occurrence")),
    ("Alcohol Abuse",                           Seq("condition_occurrence","procedure_occurrence")),
    ("Drug Abuse",                              Seq("condition_occurrence")),
    ("Psychoses",                               Seq("condition_occurrence")),
    ("Depression",                              Seq("condition_occurrence"))
  )

  pairs.flatMap { case (categoryName, tables) =>
    tables.map(table => (table, categoryName))
  }.groupBy(_._1).foreach { case (table, entries) =>
    println(table)
    println(entries.map(_._2).mkString("\n"))
    println
  }
}