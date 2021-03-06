package com.bnd.ehrop.akka

import akka.stream.scaladsl.Flow
import akka.NotUsed
import java.util.Date

import scala.collection.mutable
import scala.util.Random

object AkkaFlow {

  // four different kinds of count flows

  def count1 =
    Flow[Int].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, value) =>
        map.update(value, map.getOrElse(value, 0) + 1)
        map
    }

  def count2 =
    Flow[Int].fold[mutable.LongMap[Int]](
      mutable.LongMap[Int]()
    ) {
      case (map, value) =>
        map.update(value, map.getOrElse(value, 0) + 1)
        map
    }

  def count3(maxRange: Int) =
    Flow[Int].fold[mutable.ArraySeq[Int]](
      mutable.ArraySeq[Int](Seq.fill(maxRange)(0) :_*)
    ) {
      case (counts, value) =>
        counts.update(value, counts(value) + 1)
        counts
  }

  def count4(maxRange: Int) =
    Flow[Int].fold[mutable.WrappedArray[Int]](
      new mutable.WrappedArray.ofInt(Array.fill[Int](maxRange)(0))
    ) {
      case (counts, value) =>
        counts.update(value, counts(value) + 1)
        counts
    }

  def count1(
    fromDate: Date,
    toDate: Date
  ): Flow[(Int, Date), mutable.Map[Int, Int], _] =
    Flow[(Int, Date)].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, (id, date)) =>
        if (date.before(toDate) && date.after(fromDate)) {
          map.update(id, map.getOrElse(id, 0) + 1)
        }
        map
    }

  def count1(
    idFromToDatesMap: Map[Int, (Date, Date)]
  ): Flow[(Int, Date), mutable.Map[Int, Int], NotUsed] =
    Flow[(Int, Date)].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, (id, date)) =>
        idFromToDatesMap.get(id).foreach { case (fromDate, toDate) =>
          if (date.before(toDate) && date.after(fromDate)) {
            map.update(id, map.getOrElse(id, 0) + 1)
          }
        }
        map
    }

  def tuple3To2[A, B]: Flow[(A, B, Any), (A, B), NotUsed] =
    Flow[(A, B, Any)].map { case (a, b, _) => (a, b) }

  def filterDate[T](
    idFromToDatesMap: Map[Int, (Long, Long)]
  ): Flow[(Int, Long, T), Option[(Int, T)], NotUsed] =
    Flow[(Int, Long, T)].map {
      case (id, date, value) =>
        idFromToDatesMap.get(id).flatMap { case (fromDate, toDate) =>
          if (date > fromDate && date <= toDate) {
            Some(id, value)
          } else
            None
        }
    }

  def filterDate2[T](
    idFromToDatesMap: Map[Int, (Long, Long)]
  ): Flow[(Int, Long, T), Option[(Int, Long, T)], NotUsed] =
    Flow[(Int, Long, T)].map {
      case (id, date, value) =>
        idFromToDatesMap.get(id).flatMap { case (fromDate, toDate) =>
          if (date > fromDate && date <= toDate) {
            Some(id, date, value)
          } else
            None
        }
    }

  def countDefined[T]: Flow[Option[(Int, T)], mutable.Map[Int, Int], NotUsed] =
    Flow[Option[(Int, T)]].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, Some((id, _))) =>
        map.update(id, map.getOrElse(id, 0) + 1)
        map

      case (map, None) =>
        map
    }

  def countAll: Flow[Int, mutable.Map[Int, Int], NotUsed] =
    Flow[Int].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, id) =>
        map.update(id, map.getOrElse(id, 0) + 1)
        map
    }

  def sum: Flow[(Int, Int), mutable.Map[Int, Int], NotUsed] =
    Flow[(Int, Int)].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, (id, value)) =>
        map.update(id, map.getOrElse(id, 0) + value)
        map
    }

  def minDate: Flow[(Int, Long), mutable.Map[Int, Long], NotUsed] =
    Flow[(Int, Long)].fold[mutable.Map[Int, Long]](
      mutable.Map[Int, Long]()
    ) {
      case (map, (id, date)) =>
        map.update(id, Math.min(map.getOrElse(id, Long.MaxValue), date))
        map
    }

  def count1X(
    idFromToDatesMap: Map[Int, (Long, Long)]
  ): Flow[(Int, Long), mutable.Map[Int, Int], NotUsed] =
    Flow[(Int, Long)].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, (id, date)) =>
        idFromToDatesMap.get(id).foreach { case (fromDate, toDate) =>
          if (date < toDate && date > fromDate) {
            map.update(id, map.getOrElse(id, 0) + 1)
          }
        }
        map
    }

  @Deprecated
  def countIntDateArray1(
    idFromToDatesMap: Map[Int, (Date, Date)]
  ): Flow[Array[Any], mutable.Map[Int, Int], NotUsed] =
    Flow[Array[Any]].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, values) =>
        if (values.length == 2) {
          val id = values(0).asInstanceOf[Int]
          idFromToDatesMap.get(id).foreach { case (fromDate, toDate) =>
            val date = values(1).asInstanceOf[Date]
            if (date.before(toDate) && date.after(fromDate)) {
              map.update(id, map.getOrElse(id, 0) + 1)
            }
          }
        }
        map
    }

  def count2(
    idFromToDatesMap: Map[Int, (Date, Date)]
  ): Flow[(Int, Date), mutable.LongMap[Int], NotUsed] =
    Flow[(Int, Date)].fold[mutable.LongMap[Int]](
      mutable.LongMap[Int]()
    ) {
      case (map, (id, date)) =>
        idFromToDatesMap.get(id).foreach { case (fromDate, toDate) =>
          if (date.before(toDate) && date.after(fromDate)) {
            map.update(id, map.getOrElse(id, 0) + 1)
          }
        }
        map
    }

    Flow[Int].fold[mutable.LongMap[Int]](
      mutable.LongMap[Int]()
    ) {
      case (map, value) =>
        map.update(value, map.getOrElse(value, 0) + 1)
        map
    }

  def max[T](
    implicit ordering : Ordering[T]
  ): Flow[(Int, T), mutable.Map[Int, T], _] =
    Flow[(Int, T)].fold[mutable.Map[Int, T]](
      mutable.Map[Int, T]()
    ) {
      case (map, (id, value)) =>
        map.update(id, ordering.max(map.getOrElse(id, value), value))
        map
    }

  def lastDefined[T, V](
    implicit ordering : Ordering[T]
  ): Flow[(Int, T, Option[V]), mutable.Map[Int, (T, V)], NotUsed] =
    Flow[(Int, T, Option[V])].fold[mutable.Map[Int, (T, V)]](
      mutable.Map[Int, (T, V)]()
    ) {
      case (map, (id, value1, value2Option)) =>
        value2Option.foreach { value2 =>
          val maxValue1 = map.get(id).map(_._1).getOrElse(value1)
          if (ordering.compare(value1, maxValue1) >= 0) {
            map.update(id, (value1, value2))
          }
        }
        map
    }

  def existsIn[T](
    set: Set[T]
  ): Flow[(Int, T), mutable.Map[Int, Boolean], NotUsed] =
    Flow[(Int, T)].fold[mutable.Map[Int, Boolean]](
      mutable.Map[Int, Boolean]()
    ) {
      case (map, (id, value)) =>
        if (set.contains(value)) {
          map.update(id, true)
        }
        map
    }

  def countIn[T](
    set: Set[T]
  ): Flow[(Int, T), mutable.Map[Int, Int], NotUsed] =
    Flow[(Int, T)].fold[mutable.Map[Int, Int]](
      mutable.Map[Int, Int]()
    ) {
      case (map, (id, value)) =>
        if (set.contains(value)) {
          map.update(id, map.getOrElse(id, 0) + 1)
        }
        map
    }

  def collectDistinct[T]: Flow[(Int, T), mutable.Map[Int, mutable.Set[T]], NotUsed] =
    Flow[(Int, T)].fold[mutable.Map[Int, mutable.Set[T]]](
      mutable.Map[Int, mutable.Set[T]]()
    ) {
      case (map, (id, value)) =>
        val set = map.get(id).getOrElse {
          val newSet = mutable.Set[T]()
          map.update(id, newSet)
          newSet
        }

        set.add(value)
        map
    }

  def standardize(options: Map[Int, (Double, Double)]) =
    Flow[Seq[Option[Any]]].map { row =>
      row.zipWithIndex.map { case (value, index) =>
        value.map(value =>
          options.get(index).map { case (shift, norm) =>
            if (norm != 0) (value.asInstanceOf[Double] - shift) / norm else 0
          }
        )
      }
    }

  def calcMultiBasicStats(size: Int) =
    Flow[Seq[Option[Double]]].fold[Seq[BasicStatsAccum]](
      Seq.fill(size)(BasicStatsAccum(0, 0, 0))
    ) {
      case (accums, values) =>
        accums.zip(values).map { case (accum, value) =>
          value match {
            case Some(value) =>
              BasicStatsAccum(
                accum.sum + value,
                accum.sqSum + value * value,
                accum.count + 1
              )

            case None => accum
          }
        }
    }

  def diffs =
    Flow[Double].sliding(2).map { els => els(1) - els(0) }

  def binIndex(prevDiff: Double, diff: Double) = {
    def binIndex(relativeDiff: Double) =
      relativeDiff match {
        case x if x < -0.75 => 0
        case x if x >= -0.75 && x < -0.3333 => 1
        case x if x >= -0.3333 && x < 0.5 => 2
        case x if x >= 0.5 && x < 3 => 3
        case x if x >= 3 => 4
      }

    if (prevDiff != 0) {
      val relativeDiff = (diff - prevDiff) / prevDiff
      binIndex(relativeDiff)
    } else if ((diff - prevDiff) == 0)
      binIndex(0)
    else
      binIndex(Double.MaxValue)
  }

  def calcDiffStats: Flow[(Int, Double), mutable.Map[Int, DiffStatsAccum], NotUsed] =
    Flow[(Int, Double)].fold[mutable.Map[Int, DiffStatsAccum]](
      mutable.Map[Int, DiffStatsAccum]()
    ) {
      case (map, (id, value)) =>
        val accum = map.get(id).getOrElse(
          DiffStatsAccum(
            0, 0, 0, Double.MaxValue, Double.MinValue, None, None, mutable.ArraySeq[Int](Seq.fill(5)(0) :_*)
          )
        )

        val newAccum = accum.prev match {
          case Some(prev) =>
            val diff = value - prev
            val relativeDiffCounts = accum.relativeDiffCounts

            val newDiffCounts = accum.prevDiff match {
              case Some(prevDiff) =>
                val index = binIndex(prevDiff, diff)
                relativeDiffCounts.update(index, relativeDiffCounts(index) + 1)
                relativeDiffCounts

              case None =>
                relativeDiffCounts
            }

            DiffStatsAccum(
              accum.sum + diff,
              accum.sqSum + diff * diff,
              accum.count + 1,
              Math.min(accum.min, diff),
              Math.max(accum.max, diff),
              prev = Some(value),
              prevDiff = Some(diff),
              newDiffCounts
            )

          case None =>
            accum.copy(prev = Some(value))
        }

        map.update(id, newAccum)
        map
    }

  def calcMeanStd(accum: BasicStatsAccum): Option[(Double, Double)] =
    if (accum.count > 0) {
      val mean = accum.sum / accum.count
      val variance = (accum.sqSum / accum.count) - mean * mean
      val std = Math.sqrt(variance)

      Some((mean, std))
    } else
      None

  def calcDiffStats(accum: DiffStatsAccum): Option[DiffStats] =
    if (accum.count > 0) {
      val mean = accum.sum / accum.count
      val variance = (accum.sqSum / accum.count) - mean * mean
      val std = Math.sqrt(variance)

      val maxCount = accum.relativeDiffCounts.max
      val maxCountIndex =
        if (maxCount > 0) {
          val maxCountIndeces = accum.relativeDiffCounts.zipWithIndex.filter(_._1 == maxCount).map(_._2)
          val maxCountIndexRaw = maxCountIndeces(Random.nextInt(maxCountIndeces.size))
          Some((maxCountIndexRaw - 2).toDouble / 2)
        } else
          None

      Some(DiffStats(mean, variance, std, accum.min, accum.max, maxCountIndex))
    } else
      None
}

case class BasicStatsAccum(
  sum: Double,
  sqSum: Double,
  count: Int
)

case class DiffStatsAccum(
  sum: Double,
  sqSum: Double,
  count: Int,
  min: Double,
  max: Double,
  prev: Option[Double],
  prevDiff: Option[Double],
  relativeDiffCounts: mutable.ArraySeq[Int]
)

case class DiffStats(
  mean: Double,
  variance: Double,
  std: Double,
  min: Double,
  max: Double,
  mostFreqRelativeDiff: Option[Double]
)