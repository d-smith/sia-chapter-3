package org.sia.chapter03App

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.io.Source.fromFile

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
    
    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)
    
    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)
   
    //Load githib employees into a set
    val empPath = homeDir + "/sia/ghEmployees.txt"
    val employees = Set() ++ (
          for {
            line <- fromFile(empPath).getLines
          } yield line.trim
        )
        
    //Create a broadcast variable
    val bcEmployees = sc.broadcast(employees)
        
    //Create a filter function to filter out non github employees    
    val isEmp: (String => Boolean) = (user: String) => bcEmployees.value.contains(user)
    
    //Register the filter function as a user defined SQL function
    import sqlContext.implicits._
    val isEmployee = sqlContext.udf.register("SetContainsUdf", isEmp)
    
    val filtered = ordered.filter(isEmployee($"login"))
    
    filtered.show()
        
    
   
    
    

  }

}
