package com.jasonfeist.spark.tika

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types._
import org.apache.tika.metadata.Metadata
import org.apache.tika.sax.BodyContentHandler
import org.junit.Test
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitSuite
import org.scalatest.mockito.MockitoSugar

import scala.collection.mutable

class FieldDataExtractorTest extends JUnitSuite with MockitoSugar {

  var dataType : DataType = mock[DataType]
  var bodyContentHandler: BodyContentHandler = mock[BodyContentHandler]
  var metadata : Metadata = mock[Metadata]

  var fieldDataExtractor : FieldDataExtractor = new FieldDataExtractor()

  @Test
  def fileNameTest {
    val fileName = "foo.txt"
    val matchedFileName = fieldDataExtractor.matchedField("FILENAME",
      dataType, bodyContentHandler, fileName, metadata, mutable.Map[String, String]())
    assert(fileName === matchedFileName)
  }

  @Test
  def textExtractTest {
    val text = "blah blah blah"
    when(bodyContentHandler.toString()).thenReturn(text)
    val matchedText = fieldDataExtractor.matchedField("TEXT",
      dataType, bodyContentHandler, "", metadata, mutable.Map[String, String]())
    assert(text === matchedText)
  }

  @Test
  def unsupportedFieldTest {
    val matchedFileName = fieldDataExtractor.matchedField("foo",
      dataType, bodyContentHandler, "", metadata, mutable.Map[String, String]())
    assert(matchedFileName === null)
  }

  @Test
  def unfoundFieldTest {
    val map = mutable.Map[String, String]()
    map += "key" -> "key"

    val matchedFileName = fieldDataExtractor.matchedField("key",
      dataType, bodyContentHandler, "", metadata, mutable.Map[String, String]())
    assert(matchedFileName === null)
  }

  @Test(expected = classOf[RuntimeException])
  def unsupportedTypeTest {
    val map = mutable.Map[String, String]()
    map += "key" -> "key"
    val matchedFileName = fieldDataExtractor.matchedField("key",
      ByteType, bodyContentHandler, "", metadata, map)
  }

  @Test
  def nullValuesTest {
    val map = mutable.Map[String, String]()
    map += "key" -> "key"
    when(metadata.get("key")).thenReturn(null)
    var matchedFieldValue = fieldDataExtractor.matchedField("key",
      IntegerType, bodyContentHandler, "", metadata, map)
    assert(matchedFieldValue == null)

    matchedFieldValue = fieldDataExtractor.matchedField("key",
      StringType, bodyContentHandler, "", metadata, map)
    assert(matchedFieldValue == null)

    matchedFieldValue = fieldDataExtractor.matchedField("key",
      DateType, bodyContentHandler, "", metadata, map)
    assert(matchedFieldValue == null)

    matchedFieldValue = fieldDataExtractor.matchedField("key",
      TimestampType, bodyContentHandler, "", metadata, map)
    assert(matchedFieldValue == null)
  }

  @Test
  def timeStampParseTest {
    val sdf = new SimpleDateFormat(fieldDataExtractor.DateFormat.dateFormat)
    val map = mutable.Map[String, String]()
    map += "timestamp" -> "timestamp"

    val now = new Date()
    val formatted = sdf.format(now)
    when(metadata.get("timestamp")).thenReturn(formatted)
    val matchedFieldValue = fieldDataExtractor.matchedField("timestamp",
      TimestampType, bodyContentHandler, "", metadata, map)

    assert(matchedFieldValue.isInstanceOf[Timestamp])
    val instant = matchedFieldValue.asInstanceOf[Timestamp].getTime()
    assert(instant === now.getTime() / 1000 * 1000) //format rounds to second
  }

  @Test
  def dateParseTest {
    val sdf = new SimpleDateFormat(fieldDataExtractor.DateFormat.dateFormat)
    val map = mutable.Map[String, String]()
    map += "date" -> "date"

    val now = new Date()
    val formatted = sdf.format(now)
    when(metadata.get("date")).thenReturn(formatted)
    val matchedFieldValue = fieldDataExtractor.matchedField("date",
      DateType, bodyContentHandler, "", metadata, map)

    assert(matchedFieldValue.isInstanceOf[Date])
    val instant = matchedFieldValue.asInstanceOf[Date].getTime()
    assert(instant === now.getTime() / 1000 * 1000) //format rounds to second
  }

}
