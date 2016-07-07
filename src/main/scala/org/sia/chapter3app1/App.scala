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

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    
    val ghLog = sqlContext.read.json(args(0))
    
    val pushes = ghLog.filter("type = 'PushEvent'")
    
    
    pushes.printSchema
    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)
    pushes.show(5)
    
    val grouped = pushes.groupBy("actor.login").count    
    val ordered = grouped.orderBy(grouped("count").desc)
   
    //Load githib employees into a set
    val employees = Set() ++ (
          for {
            line <- fromFile(args(1)).getLines
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
    filtered.write.format(args(3)).save(args(2))
    

  }

}
