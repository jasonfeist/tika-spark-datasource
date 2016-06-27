package com.jasonfeist.spark.tika

import org.apache.spark.input.PortableDataStream
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.BodyContentHandler


class MetadataExtractor extends Serializable {

  def extract(
               file: (String, PortableDataStream)
             ) : (BodyContentHandler, Metadata) = {

    val tis = TikaInputStream.get(file._2.open())
    val parser = new AutoDetectParser()
    val handler = new BodyContentHandler(-1)
    val metadata = new Metadata()
    parser.parse(tis, handler, metadata, new ParseContext())
    (handler, metadata)
  }

}
