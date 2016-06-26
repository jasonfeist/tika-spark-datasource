package com.jasonfeist.spark.tika

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructType}
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.BodyContentHandler
import org.slf4j.LoggerFactory


class TikaMetadataRelation protected[tika] (path: String, userSchema: StructType)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  val logger = LoggerFactory.getLogger(classOf[TikaMetadataRelation])

  override def schema: StructType = this.userSchema

  override def buildScan(): RDD[Row] = {

    val rdd = sqlContext
      .sparkContext.binaryFiles(path)
    rdd.map(extractFunc(_));
  }

  def extractFunc(
                    file: (String, PortableDataStream)
                  ) : Row  =
  {

    val rowArray = new Array[Any](schema.fields.length)
    val tis = TikaInputStream.get(file._2.open())
    var index = 0
    val parser = new AutoDetectParser();
    val handler = new BodyContentHandler(-1);
    val metadata = new Metadata();
    parser.parse(tis, handler, metadata, new ParseContext());
    while (index < schema.fields.length) {
      val field = schema(index)
      def matchedField(field: String): String = field match {
        case "Language" => {
          val languageObj = new LanguageIdentifier(handler.toString())
          languageObj.getLanguage()
        }
        case "DetectedType" => {
          val tika = new Tika()
          tika.detect(file._1)
        }
        case "FileName" => {
          file._1
        }
        case "Text" => {
          handler.toString()
        }
        case _ => {
          val v:Option[String] = Some(metadata.get(field))
          v.getOrElse("")
        }
      }
      rowArray(index) = matchedField(field.name)
      index = index + 1
    }
    Row.fromSeq(rowArray)
  }
}
