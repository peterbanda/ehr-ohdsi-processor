package com.bnd.ehrop

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import akka.stream.scaladsl.{FileIO, Flow, Framing, Source}
import akka.util.ByteString
import org.apache.commons.io.IOUtils

object AkkaFileSource {

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
    source.prefixAndTail(1).flatMapConcat { case (source, tail) =>
      val header = source.head.split(delimiter, -1)
      val processEls = withHeaderTrans(header)
      tail.map { line =>
        val els = line.split(delimiter, -1)
        processEls(els)
      }
    }
  }

  private def fileSource(
    fileName: String,
    eol: String,
    allowTruncation: Boolean
  ) =
    FileIO.fromPath(Paths.get(fileName))
      .via(Framing.delimiter(ByteString(eol), 1000000, allowTruncation)
        .map(_.utf8String))

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
