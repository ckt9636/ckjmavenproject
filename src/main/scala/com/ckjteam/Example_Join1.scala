package com.ckjteam

import org.apache.spark.sql.SparkSession

object Example_Join1 {
  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").


    getOrCreate()

  var staticUrl1 = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser1 = "kopo"
  var staticPw1 = "kopo"
  var selloutDb1 = "kopo_region_mst"


  val selloutDataFromPg1= spark.read.format("jdbc").
    options(Map("url" -> staticUrl1,"dbtable" -> selloutDb1,"user" -> staticUser1, "password" -> staticPw1)).load
  selloutDataFromPg1.createOrReplaceTempView("selloutTable1")
  selloutDataFromPg1.show(2)



  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"


  val selloutDataFromPg= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
  selloutDataFromPg.createOrReplaceTempView("selloutTable")
  selloutDataFromPg.show(2)

   //left join      없는 데이터도 일단 묶고 나중에 처리하는 것
  spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from selloutTable a " +
    "left join selloutTable1 b " +
    "on a.regionid = b.regionid")

  //inner join      없는 데이터를 버리고 묶기
  spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from selloutTable a " +
    "inner join selloutTable1 b " +
    "on a.regionid = b.regionid")


}
