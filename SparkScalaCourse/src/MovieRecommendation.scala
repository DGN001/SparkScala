package com.deepak.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._

import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


object MovieRecommendation {
  
   /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  type MovieData = (Int,(Int,Int))
   /** Load up a Map of (UserID, (movieID , Rating))*/
  def maptoMoviewData(lines : String) : MovieData ={
    return (lines.split("\t")(0).toInt,(lines.split("\t")(1).toInt,lines.split("\t")(2).toInt))
  }
  def main(args :Array[String]) ={
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "MovieRecommentation")  
  // Create a broadcast variable of our ID -> movie name map
  var nameDict = sc.broadcast(loadMovieNames)
  
  val lines = sc.textFile("../ml-100k/u.data")
  val rating = lines.map(l => l.split("\t")).map(l => (l(0).toInt,(l(1).toInt,l(2).toDouble)))//maptoMoviewData)
  val userRating = rating.join(rating)
  val result = rating.collect()
  result.foreach(println)
  }
}