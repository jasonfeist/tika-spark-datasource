package com.jasonfeist.spark.tika

import org.apache.spark.input.PortableDataStream
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.BodyContentHandler

import scala.collection.mutable


class MetadataExtractor extends Serializable {

  def extract(
               file: (String, PortableDataStream)
             ) : (BodyContentHandler, Metadata, mutable.Map[String, String]) = {

    val tis = TikaInputStream.get(file._2.open())
    val parser = new AutoDetectParser()
    val handler = new BodyContentHandler(-1)
    val metadata = new Metadata()
    parser.parse(tis, handler, metadata, new ParseContext())
    val lowerCaseToCaseSensitive = mutable.Map[String, String]()
    for (name <- metadata.names()) {
      lowerCaseToCaseSensitive += (name.toLowerCase -> name)
    }
    (handler, metadata, lowerCaseToCaseSensitive)
  }

}
