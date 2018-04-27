package com.ckjteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import org.apache.spark.sql.Row

object testModule {
  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").getOrCreate()


  // oracle connection
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var productNameDb = "kopo_product_mst"

  val selloutDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  val productMasterDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  selloutDf.createOrReplaceTempView("selloutTable")
  productMasterDf.createOrReplaceTempView("mstTable")

  var rawData = spark.sql("select " +
    "concat(a.regionid,'_',a.product) as keycol, " +
    "a.regionid as accountid, " +
    "a.product, " +
    "a.yearweek, " +
    "cast(a.qty as double) as qty, " +
    "b.product_name " +
    "from selloutTable a " +
    "left join mstTable b " +
    "on a.product = b.product_id")

  rawData.show(2)

 // var rawData = spark.sql("select regionid " +
  //  "qty * 1.2 AS QTY_NEW FROM selloutTable")
 // var rawData = spark.sql("select regionid,  YEARWEEK, QTY " +
  //  ", qty * 1.2 AS QTY_NEW FROM selloutTable")




  var rawDataColumns = rawData.columns
  var keyNo = rawDataColumns.indexOf("keycol")
  var accountidNo = rawDataColumns.indexOf("accountid")
  var productNo = rawDataColumns.indexOf("product")
  var yearweekNo = rawDataColumns.indexOf("yearweek")
  var qtyNo = rawDataColumns.indexOf("qty")
  var productnameNo = rawDataColumns.indexOf("product_name")

  // (keyol, accountid, product, yearweek, qty, product_name)
  var rawRdd = rawData.rdd

  var filteredRdd = rawRdd.filter(x=>{
    // boolean = true
    var checkValid = true
    // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
    var weekValue = x.getString(yearweekNo).substring(4).toInt

    // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
    if( weekValue >= 53){
      checkValid = false
    }
    checkValid
  })
  //분석대상 제품군 등록
  var productArray = Array("PRODUCT1","PRODUCT2")
// 세트 타입으로 변환
var productSet = productArray.toSet

var resultRdd = filteredRdd.filter(x=>{
  var checkValid = false

  var productInfo = x.getString(productNo);

 if (productSet.contains(productInfo)){
    checkValid = true
  }
  checkValid
})

  //2번째 답
 // if( (productInfo == "product1") ||
 //   (productInfo == "product2")) {
 //    checkValid = true
 // }
//  checkValid
//})

//filteredRdd.take(2).foreach(println)



//val finalResultDf = spark.createDataFrame(resultRdd,
//StructType(
//Seq(
//StructField("KEY", StringType),
//StructField("REGIONID", StringType),
//StructField("PRODUCT", StringType),
//StructField("YEARWEEK", StringType),
//StructField("VOLUME", DoubleType),
//StructField("PRODUCT_NAME", StringType))))


//filteredRdd.first
//filteredRdd= (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)

//처리로직 : 거래량이 MAXVALUE 이상인 건은 MAXVALUE로 치환한다.
var MAXVALUE = 700000

  var mapRdd = filteredRdd.map(c=>{
    //디버깅 코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyno) > 700000}).first
    var qty = x.getString(qtyNo)
})


var mapRdd = rawRdd.map(x=>{
var qty = x.getString(qtyNo)
var maxValue = 700000
if(qty > 700000){qty = 700000}
Row( x.getString(keyNo),
x.getString(yearweekNo),
qty, //x.getString(qtyNo)
})

