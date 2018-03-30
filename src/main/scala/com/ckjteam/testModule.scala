package com.ckjteam

import org.apache.spark.sql.SparkSession

object testModule {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")

  }
}
