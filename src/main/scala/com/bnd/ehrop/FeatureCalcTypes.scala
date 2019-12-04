package com.bnd.ehrop

import scala.collection.mutable
import _root_.akka.stream.scaladsl.{Flow, Sink, Source}
import _root_.akka.NotUsed

object FeatureCalcTypes {
  // id, date, and additional data
  type EHRData = (Int, Long, Seq[Option[Int]])
  type EHRFlow[T] = Flow[EHRData, mutable.Map[Int, T], NotUsed]
  type SeqEHRFlow[T] = Flow[EHRData, Seq[mutable.Map[Int, T]], NotUsed]
}