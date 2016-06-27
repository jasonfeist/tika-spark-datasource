package com.jasonfeist.spark.tika

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructType}
import org.slf4j.LoggerFactory


class TikaMetadataRelation protected[tika] (path: String,
                                            userSchema: StructType,
                                            metadataExtractor: MetadataExtractor,
                                            fieldDataExtractor: FieldDataExtractor)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  val logger = LoggerFactory.getLogger(classOf[TikaMetadataRelation])

  override def schema: StructType = this.userSchema

  override def buildScan(): RDD[Row] = {

    val rdd = sqlContext
      .sparkContext.binaryFiles(path)
    rdd.map(extractFunc(_))
  }

  def extractFunc(
                    file: (String, PortableDataStream)
                  ) : Row  =
  {
    val extractedData = metadataExtractor.extract(file)
    val rowArray = new Array[Any](schema.fields.length)
    var index = 0
    while (index < schema.fields.length) {
      val field = schema(index)
      val fieldData = fieldDataExtractor.matchedField(field.name,
        field.dataType, extractedData._1, file._1, extractedData._2,
        extractedData._3)
      rowArray(index) = fieldData
      index = index + 1
    }
    Row.fromSeq(rowArray)
  }
}
