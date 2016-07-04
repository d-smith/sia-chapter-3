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
    
    val homeDir = System.getenv("HOME") 
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json" 
    val ghLog = sqlContext.read.json(inputPath)
    
    val pushes = ghLog.filter("type = 'PushEvent'")
    
    
    pushes.printSchema
    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)
    pushes.show(5)
    
    val groups = pushes.groupBy("payload.commits.author").count
    groups.show(5)
    
    val ordered = groups.orderBy(groups("count").desc) 
    ordered.show(5)

  }

}
