package com.ckjteam
import org.apache.spark.sql.SparkSession

object Example_DataLoading {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()



    var staticUrl = "jdbc:oracle:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    //jdbc(java database connectivity)
   // val selloutData = spark.read.format("jdbc").
    //  options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
   // selloutData.createTempView("maindata")
    //selloutData.show(1)
   val selloutDataFromPg= spark.read.format("jdbc").
     options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromPg.createOrReplaceTempView("selloutTable")
    selloutDataFromPg.show(1)


  }
  }