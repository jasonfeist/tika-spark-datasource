package com.jasonfeist.spark.tika.example

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jason on 6/26/16.
  */
object TikaLanguageAggregationExample {

  def main(args: Array[String]) {
    if (args.length == 0 || args(0) == null) {
      return
    }
    val conf = new SparkConf().setAppName("Tika Language Aggregation Example")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read
      .format("com.jasonfeist.spark.tika")
      .load(args(0))
      .groupBy("Language")
      .count()
    df.show
  }
}
