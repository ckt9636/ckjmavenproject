package com.ckjteam

import org.apache.spark.sql.SparkSession;

object Example_Join {

  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").
    getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"


  var mainDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + mainData)
  var subDataDf = spark.read.format("csv").
    option("header", "true").
    load(dataPath + subData)

  mainDataDf.createOrReplaceTempView("mainTable")
  subDataDf.createOrReplaceTempView("subTable")



spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
 "from mainData a " + "inner join subData b " +
"on a.productgroup = b.productid")







}
