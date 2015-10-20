package org.apache.spark.recommendation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math._
import java.io._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scala.util.control._
import java.util.Properties

object Recommendation {

	def parseItemSim(line:String) : String = {
		val movieId = line.split("\t")(0)
		val simMovies = line.split("\t")(1)
		
		var simMovieIds = new String()
		simMovies.split(" ").foreach(s=>simMovieIds+=s.split(":")(0)+" ")

		simMovieIds = simMovieIds.split(" ").mkString(",")
		
		return movieId.toString+"\t"+simMovieIds

	}

	def parseItemSimMovieNames(line:String, movieIDName: Map[String, String]) : String = {
                val movieId = line.split("\t")(0)
                val simMovies = line.split("\t")(1)

                var simMovieIds = new String()
                simMovies.split(" ").foreach(s=>simMovieIds+=s.split(":")(0)+":"+movieIDName.get(s.split(":")(0))+" ")

                simMovieIds = simMovieIds.split(" ").mkString(",")

                return movieId.toString+":"+movieIDName.get(movieId.toString)+"\n"+simMovieIds+"\n"

        }


	def main(args: Array[String]) {
		
		val conf = new SparkConf().setAppName("Recommendation")
        	conf.setMaster("local[2]")
        	val sc = new SparkContext(conf)
		
        	val ratingsRDD = sc.textFile("/home/hduser/recommendation/src/main/scala/ratings.dat")
		val moviesRDD = sc.textFile("/home/hduser/recommendation/src/main/scala/movies.dat")
		val itemsim_matrixRDD = sc.textFile("/home/hduser/recommendation/src/main/scala/similarity-matrix/part-00000")

		val userID:Int = 20
		val rating:Double = 3.0

		val filteredMovieIds = ratingsRDD.filter(line=>(line.split("::")(0).toInt == userID && line.split("::")(2).toDouble == rating)).map(m=>m.split("::")(1)).collect()
		//filteredMovieIds.foreach(println)

		val filteredItemSimMatrix = itemsim_matrixRDD.filter(line=>(filteredMovieIds.contains(line.split("\t")(0))))
		//filteredItemSimMatrix.foreach(println)

		val parsedFilteredItemSimMatrix = filteredItemSimMatrix.map(line=>parseItemSim(line))
		//parsedFilteredItemSimMatrix.saveAsTextFile("movieIdlist")

		var movieNames = moviesRDD.map(s=>(s.split("::")(0), s.split("::")(1))).collect.toMap

		val itemSimMatrixWithMovieNames = filteredItemSimMatrix.map(line=>parseItemSimMovieNames(line, movieNames))

		//itemSimMatrixWithMovieNames.foreach(println)
		itemSimMatrixWithMovieNames.saveAsTextFile("movieIDNameList")
	}

}
