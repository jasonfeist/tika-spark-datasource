package com.jasonfeist.spark.tika

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class DefaultSource
  extends RelationProvider with SchemaRelationProvider  {

    def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
      parameters.getOrElse("path", sys.error("No path specified."))
      new TikaMetadataRelation(parameters.get("path").get,schema)(sqlContext)
    }

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
      val struct =
        StructType(
              StructField("DetectedType", StringType, true) ::
              StructField("Language", StringType, true) ::
              StructField("FileName", StringType, true) ::
              StructField("Author", StringType, true)  ::
              StructField("Text", StringType, true)  ::
              StructField("Creation-Date", StringType, true) ::
              StructField("Title", StringType, true) ::
              StructField("Content-Length", StringType, true) ::
              StructField("Last-Modified", StringType, true) :: Nil
        )
      createRelation(sqlContext, parameters, struct)
    }
  }
