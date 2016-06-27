package com.jasonfeist.spark.tika

import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.types._
import org.apache.tika.Tika
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.BodyContentHandler

import scala.collection.mutable

class FieldDataExtractor extends Serializable {

  val dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  def matchedField(field: String,
                   castType: DataType,
                   bodyContentHandler: BodyContentHandler,
                   fileName: String,
                   metadata: Metadata,
                   lowerCaseToCaseSensitive: mutable.Map[String, String]
                  ): Object = {
      if ("language" == field.toLowerCase) {
        val languageObj = new LanguageIdentifier(bodyContentHandler.toString())
        languageObj.getLanguage()
      } else if ("detectedtype" == field.toLowerCase) {
        val tika = new Tika()
        tika.detect(fileName)
      } else if ("filename" == field.toLowerCase) {
        fileName
      } else if ("text" == field.toLowerCase) {
        bodyContentHandler.toString()
      } else {
          val caseInsensitiveField = lowerCaseToCaseSensitive.get(field.toLowerCase).getOrElse(return null)
          castType match {
            case _: IntegerType =>
              val v = metadata.get(caseInsensitiveField)
              if (v == null) {
                return null
              }
              Integer.parseInt(v).asInstanceOf[Object]

            case _: StringType =>
              metadata.get(caseInsensitiveField)

            case _: DateType =>
              val sdf = new SimpleDateFormat(dateFormat)
              val v = metadata.get(caseInsensitiveField)
              if (v == null) {
                return null
              }
              new Date(sdf.parse(v).getTime)

            case _: TimestampType =>
              val sdf = new SimpleDateFormat(dateFormat)
              val v = metadata.get(caseInsensitiveField)
              if (v == null) {
                return null
              }
              new Timestamp(sdf.parse(v).getTime)

            case _ => throw new RuntimeException(s"Type not supported: ${castType.typeName}")
          }
      }
  }

}
