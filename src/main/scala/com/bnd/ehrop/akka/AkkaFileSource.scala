package com.bnd.ehrop.akka

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.util.{Calendar, Date, TimeZone}

import akka.stream.{IOResult, Materializer}
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.IOUtils

import scala.concurrent.Future

object AkkaFileSource {

  // logger
  protected val logger = Logger(this.getClass.getSimpleName)
  val defaultTimeZone = TimeZone.getTimeZone("CET") // CEST

  def csvAsSource(
    fileName: String,
    delimiter: String = ",",
    eol: String = "\n",
    allowTruncation: Boolean = true
  ): Source[Array[String], _] = {
    // file source
    val source = fileSource(fileName, eol, allowTruncation)

    // skip head and split lines
    source.prefixAndTail(1).flatMapConcat { case (_, tail) =>
      tail.map(_.split(delimiter, -1))
    }
  }

  def csvAsSourceWithTransform[T](
    fileName: String,
    withHeaderTrans: Array[String] => Array[String] => T,
    delimiter: String = ",",
    eol: String = "\n",
    allowTruncation: Boolean = true
  ): Source[T, _] = {
    // file source
    val source = fileSource(fileName, eol, allowTruncation)

    // skip head, split lines, and apply a given transformation
    source.prefixAndTail(1).flatMapConcat { case (first, tail) =>
      val header = first.head.split(delimiter, -1)
      val processEls = withHeaderTrans(header)
      tail.map { line =>
        val els = line.split(delimiter, -1)
        processEls(els)
      }
    }
  }

  // keep the header
  def csvAsStringSourceWithTransformAndHeader(
    fileName: String,
    withHeaderTrans: Array[String] => Array[String] => String,
    delimiter: String = ",",
    eol: String = "\n",
    allowTruncation: Boolean = true
  ): Source[String, _] = {
    // file source
    val source = fileSource(fileName, eol, allowTruncation)

    // skip head, split lines, and apply a given transformation
    source.prefixAndTail(1).flatMapConcat { case (first, tail) =>
      val header = first.head.split(delimiter, -1)
      val processEls = withHeaderTrans(header)
      val processed = tail.map { line =>
        val els = line.split(delimiter, -1)
        processEls(els)
      }

      Source(first).concat(processed)
    }
  }

  private def indexColumnSafe(
    columnName: String,
    columnIndexMap: Map[String, Int],
    inputPath: String
  ) =
    columnIndexMap.get(columnName).getOrElse(throw new IllegalArgumentException(s"Column '${columnName}' in the file '${inputPath}' not found"))

  def intCsvSource(
    inputPath: String,
    columnName: String
  ): Source[Int, _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = indexColumnSafe(columnName, columnIndexMap, inputPath)

        def int(els: Array[String]) = els(intColumnIndex).trim

        els => int(els).toDouble.toInt
      }
    )

  def ehrDataCsvSource(
    inputPath: String,
    idColumnName: String,
    dateColumnName: String,
    dataIntColumnNames: Seq[String],
    timeZone: TimeZone = defaultTimeZone
  ): Source[(Int, Option[Long], Seq[Option[Int]]), _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val idColumnIndex = indexColumnSafe(idColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)
        val dataColumnIndeces = dataIntColumnNames.map(indexColumnSafe(_, columnIndexMap, inputPath))

        def asInt(string: String) = string.toDouble.toInt
        def asIntOptional(string: String) = if (string.nonEmpty) Some(asInt(string)) else None

        def id(els: Array[String]) = asInt(els(idColumnIndex).trim)
        def date(els: Array[String]): Option[Long] = asDateMilis(els(dateColumnIndex).trim, inputPath, timeZone)
        def intData(els: Array[String]) = dataColumnIndeces.map(index => asIntOptional(els(index).trim))

        els =>
          try {
            (id(els), date(els), intData(els))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${idColumnName}', '${dateColumnName}', '${dateColumnName.mkString(", ")}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  def intDateCsvSource(
    inputPath: String,
    intColumnName: String,
    dateColumnName: String,
    timeZone: TimeZone = defaultTimeZone
  ): Source[Option[(Int, Date)], _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = indexColumnSafe(intColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)

        def int(els: Array[String]) = els(intColumnIndex).trim.toDouble.toInt
        def dateSafe(els: Array[String]): Option[Date] = asDate(els(dateColumnIndex).trim, inputPath, timeZone)

        els =>
          try {
            dateSafe(els).map((int(els), _))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${intColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e
          }
      }
    )

  def intMilisDateCsvSource(
    inputPath: String,
    intColumnName: String,
    dateColumnName: String,
    timeZone: TimeZone = defaultTimeZone
  ): Source[Option[(Int, Long)], _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val intColumnIndex = indexColumnSafe(intColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)

        def int(els: Array[String]) = els(intColumnIndex).trim.toDouble.toInt
        def dateSafe(els: Array[String]): Option[Long] = asDateMilis(els(dateColumnIndex).trim, inputPath, timeZone)

        els =>
          try {
            dateSafe(els).map((int(els), _))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with columns '${intColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  def asDateMilis(
    dateString: String,
    inputPath: String,
    timeZone: TimeZone = defaultTimeZone
  ) =
    asCalendar(dateString, inputPath, timeZone).map(_.getTimeInMillis)

  def asDate(
    dateString: String,
    inputPath: String,
    timeZone: TimeZone = defaultTimeZone
  ) =
    asCalendar(dateString, inputPath, timeZone).map(_.getTime)

  protected def asCalendar(
    dateString: String,
    inputPath: String,
    timeZone: TimeZone = defaultTimeZone
  ) =
    if (dateString.nonEmpty) {
      val date = try {
        val year = dateString.substring(0, 4).toInt
        val month = dateString.substring(5, 7).toInt
        val day = dateString.substring(8, 10).toInt

        val calendar = Calendar.getInstance(timeZone)
        calendar.set(Calendar.YEAR, year)
        calendar.set(Calendar.MONTH, month - 1)
        calendar.set(Calendar.DAY_OF_MONTH, day)
        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        calendar
      } catch {
        case e: ParseException =>
          logger.error(s"Cannot parse a date string '${dateString}' for the path '${inputPath}'.", e)
          throw e

        case e: Exception =>
          logger.error(s"Fatal problem for a date string '${dateString}' and the path '${inputPath}'.", e)
          throw e
      }
      Some(date)
    } else {
      None
    }

  def fileSource(
    fileName: String,
    eol: String,
    allowTruncation: Boolean
  ) =
    FileIO.fromPath(Paths.get(fileName))
      .via(Framing.delimiter(ByteString(eol), 1000000, allowTruncation)
        .map(_.utf8String))

  def writeLines(
    source: Source[String, _],
    fileName: String)(
    implicit materializer: Materializer
  ): Future[IOResult] =
    source.map(line => ByteString(line + "\n")).runWith(FileIO.toPath(Paths.get(fileName)))

  def writeStringAsStream(string: String, file: File) = {
    val outputStream = Stream(string.getBytes(StandardCharsets.UTF_8))
    writeByteArrayStream(outputStream, file)
  }

  def writeByteArrayStream(data: Stream[Array[Byte]], file : File) = {
    val target = new BufferedOutputStream(new FileOutputStream(file))
    try
      data.foreach(IOUtils.write(_, target))
    finally
      target.close
  }
}
