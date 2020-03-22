package com.deepak.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  
  def parseLines(x : String) ={
    val Fields = x.split(",")
    val name = Fields(1).toString
    val fcount = Fields(2).toInt
    (name,fcount)
  }
  
  def main(args : Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","FriendsByName")
    val dataRdd = sc.textFile("../fakefriends.csv")
    val mapRdd = dataRdd.map(parseLines)
    val reduceRdd = mapRdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1,x._2+y._2 ))
    val finalRdd = reduceRdd.mapValues(x => x._1 / x._2)
    val result = finalRdd.collect()
    result.sorted.foreach(println)
    
  }
  
}