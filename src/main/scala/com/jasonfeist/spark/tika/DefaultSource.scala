package com.jasonfeist.spark.tika

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types._


class DefaultSource
  extends RelationProvider with SchemaRelationProvider  {

    def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
      parameters.getOrElse("path", sys.error("No path specified."))
      new TikaMetadataRelation(
        parameters.get("path").get,
        schema,
        new MetadataExtractor(),
        new FieldDataExtractor())(sqlContext)
    }

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
      val struct =
        StructType(
              StructField("detectedtype", StringType, true) ::
              StructField("language", StringType, true) ::
              StructField("filename", StringType, true) ::
              StructField("author", StringType, true)  ::
              StructField("text", StringType, true)  ::
              StructField("creation-date", TimestampType, true) ::
              StructField("title", StringType, true) ::
              StructField("content-length", IntegerType, true) ::
              StructField("last-modified", DateType, true) :: Nil
        )
      createRelation(sqlContext, parameters, struct)
    }
  }
