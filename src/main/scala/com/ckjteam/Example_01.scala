package com.ckjteam

import org.apache.spark.sql.SparkSession

object Example_01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    //접속정보 설정
    //var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    //var staticUser = "kopo"
    //var staticPw = "kopo"
    //var selloutDb = "kopo_channel_seasonality"

    // 파일설정
    //var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"

    //var staticUser = "root"
    //var staticPw = "P@ssw0rd"
    //var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
    //val selloutDataFromMysql= spark.read.format("jdbc").
      //options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    //selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    //selloutDataFromMysql.show

    // 파일설정
   // var staticUrl = "jdbc:sqlserver://127.0.0.1;databaseName=kopo"
   // var staticUser = "kopo"
   // var staticPw = "kopo"
   // var selloutDb = "kopo_channel_seasonality"

    // jdbc (java database connectivity) 연결
   // val selloutDataFromSqlserver= spark.read.format("jdbc").
   //   options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
   // selloutDataFromSqlserver.registerTempTable("selloutTable")
   // selloutDataFromSqlserver.show

    // 접속정보 설정
   // var staticUrl = "jdbc:oracle:thin:@192.168.110.13:1522/XE"
   // var staticUser = "ckj"
   // var staticPw = "ckj"
   // var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity) 연결
   // val selloutDataFromOracle= spark.read.format("jdbc").
   //   options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    //selloutDataFromOracle.createOrReplaceTempView("KOPO_PRODUCT_VOLUME")
   // selloutDataFromOracle.show
    //var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    //var staticUser = "kopo"
    //var staticPw = "kopo"
    //var selloutDb = "kopo_batch_season_mpara"

    // jdbc (java database connectivity) 연결
    //val selloutDataFromPg=spark.read.format("jdbc").
      //options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
      //selloutDataFromPg.createOrReplaceTempView("pgParamTable")
   // selloutDataFromPg.show

    // 데이터베이스 주소 및 접속정보 설정
    //var outputUrl = "jdbc:oracle:thin:@192.168.110.13:1522/XE"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    //var outputUser = "CKJ"
    //var outputPw = "ckj"

    // 데이터 저장
    //var prop = new java.util.Properties
    //prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    //prop.setProperty("user", outputUser)
    //prop.setProperty("password", outputPw)
    //var table = "sssss"
    //append
    //selloutDataFromPg.write.mode("overwrite").jdbc(outputUrl, table, prop)
    //selloutDataFromPg.show
    












  }
}
