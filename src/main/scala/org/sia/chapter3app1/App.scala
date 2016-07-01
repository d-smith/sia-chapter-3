package org.sia.chapter03App

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext;

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("Github push counter")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    
    println(System.getenv("SPARK_HOME"))
    
    val homeDir = System.getenv("HOME") 
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json" 
    val ghLog = sqlContext.jsonFile(inputPath)
    
    val pushes = ghLog.filter("type = 'PushEvent'")
    pushes.printSchema
  }

}
