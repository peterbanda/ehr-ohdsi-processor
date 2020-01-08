package com.bnd.ehrop.akka

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.text.{ParseException, ParsePosition, SimpleDateFormat}
import java.util.{Calendar, Date, TimeZone}

import akka.stream.{IOResult, Materializer}
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import com.bnd.ehrop.BasicHelper
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.IOUtils

import scala.concurrent.{ExecutionContext, Future}

object AkkaFileSource extends BasicHelper {

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
    newHeader: Array[String] => Array[String] = identity,
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

      val newHeaderString = newHeader(header).mkString(delimiter)
      Source(List(newHeaderString)).concat(processed)
    }
  }

  def ehrDataCsvSource(
    inputPath: String,
    idColumnName: String,
    dateColumnName: String,
    dataIntColumnNames: Seq[String],
    dateStoredAsMilis: Boolean = false,
    timeZone: TimeZone = defaultTimeZone
  ): Source[(Int, Option[Long], Seq[Option[Int]]), _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        // column indeces for a quick lookup
        val columnIndexMap = header.zipWithIndex.toMap
        val idColumnIndex = indexColumnSafe(idColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)
        val dataColumnIndeces = dataIntColumnNames.map(indexColumnSafe(_, columnIndexMap, inputPath))

        // aux functions to get values for the columns
        def id(els: Array[String]) =
          asInt(els, idColumnIndex)

        def date(els: Array[String]) =
          asDateMilis(els, dateColumnIndex, inputPath, timeZone, dateStoredAsMilis)

        def intData(els: Array[String]) =
          dataColumnIndeces.map(index => asIntOptional(els, index))

        els =>
          try {
            (id(els), date(els), intData(els))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with the columns '${idColumnName}', '${dateColumnName}', '${dateColumnName.mkString(", ")}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  def idMilisDateCsvSource(
    inputPath: String,
    idColumnName: String,
    dateColumnName: String,
    dateStoredAsMilis: Boolean = false,
    timeZone: TimeZone = defaultTimeZone
  ): Source[Option[(Int, Long)], _] =
    csvAsSourceWithTransform(inputPath,
      header => {
        // column indeces for a quick lookup
        val columnIndexMap = header.zipWithIndex.toMap
        val idColumnIndex = indexColumnSafe(idColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)

        // aux functions to get values for the columns
        def id(els: Array[String]) =
          asInt(els, idColumnIndex)

        def date(els: Array[String]) =
          asDateMilis(els, dateColumnIndex, inputPath, timeZone, dateStoredAsMilis)

        els =>
          try {
            date(els).map((id(els), _))
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with the columns '${idColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e;
          }
      }
    )

  // with header
  def milisDateEhrDataStringCsvSource(
    inputPath: String,
    idColumnName: String,
    dateColumnName: String,
    dataIntColumnNames: Seq[String],
    timeZone: TimeZone
  ) =
    AkkaFileSource.csvAsStringSourceWithTransformAndHeader(inputPath,
      header => {
        val columnIndexMap = header.zipWithIndex.toMap
        val idColumnIndex = indexColumnSafe(idColumnName, columnIndexMap, inputPath)
        val dateColumnIndex = indexColumnSafe(dateColumnName, columnIndexMap, inputPath)
        val dataColumnIndeces = dataIntColumnNames.map(indexColumnSafe(_, columnIndexMap, inputPath))


        def id(els: Array[String]) = els(idColumnIndex).trim
        def dateMilis(els: Array[String]): Option[Long] = asDateMilis(els(dateColumnIndex).trim, inputPath, timeZone)
        def data(els: Array[String]) = dataColumnIndeces.map(index => els(index).trim)

        els =>
          try {
            (Seq(dateMilis(els).getOrElse(""), id(els)) ++ data(els)).mkString(",")
          } catch {
            case e: Exception =>
              logger.error(s"Error while processing an file with the columns '${idColumnName}' and '${dateColumnName}' at the path '${inputPath}'.", e)
              throw e;
          }
      },
      newHeader = _ => (Seq(dateColumnName, idColumnName) ++ dataIntColumnNames).toArray
    )

  private def asInt(els: Array[String], index: Int) =
    els(index).trim.toDouble.toInt

  private def asIntOptional(els: Array[String], index: Int) = {
    val string = els(index).trim
    if (string.nonEmpty) Some(string.toDouble.toInt) else None
  }

  private def asLongOptional(string: String) =
    if (string.nonEmpty) Some(string.toLong) else None

  def asDateMilis(
    els: Array[String],
    index: Int,
    inputPath: String,
    timeZone: TimeZone,
    dateStoredAsMilis: Boolean): Option[Long] = {
    val string = els(index).trim

    if (dateStoredAsMilis)
      asLongOptional(string)
    else
      asDateMilis(string, inputPath, timeZone)
  }

  def asDateMilis(
    dateString: String,
    inputPath: String,
    timeZone: TimeZone = defaultTimeZone
  ): Option[Long] =
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
  ): Option[Calendar] =
    if (dateString.nonEmpty) {
      val date = try {
        val year = dateString.substring(0, 4).toInt
        val month = dateString.substring(5, 7).toInt
        val day = dateString.substring(8, 10).toInt
        toCalendar(year, month, day, timeZone)
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

  def toCalendar(
    year: Int,
    month: Int,
    day: Int,
    timeZone: TimeZone = defaultTimeZone
  ): Calendar = {
    val calendar = Calendar.getInstance(timeZone)
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month - 1)
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    calendar
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

  private def indexColumnSafe(
    columnName: String,
    columnIndexMap: Map[String, Int],
    inputPath: String
  ) =
    columnIndexMap.get(columnName).getOrElse(throw new IllegalArgumentException(s"Column '${columnName}' in the file '${inputPath}' not found"))
}
