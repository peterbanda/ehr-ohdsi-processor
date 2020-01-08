package com.bnd.ehrop

import com.bnd.ehrop.akka.AkkaFileSource
import _root_.akka.actor.ActorSystem
import _root_.akka.stream.ActorMaterializer
import com.bnd.ehrop.akka.AkkaFileSource.{asDateMilis, asInt, indexColumnSafe, logger}
import com.bnd.ehrop.model.{OutputColumn, Table, TimeLagStats}

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