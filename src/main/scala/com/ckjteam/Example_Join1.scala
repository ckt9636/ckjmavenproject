package com.ckjteam

import org.apache.spark.sql.SparkSession

object Example_Join1 {
  val spark = SparkSession.builder().appName("...").
    config("spark.master", "local").
    getOrCreate()

}
