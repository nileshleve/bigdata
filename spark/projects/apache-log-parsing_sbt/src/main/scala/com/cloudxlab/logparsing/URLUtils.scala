package com.cloudxlab.logparsing
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class URLUtils extends Serializable {

    //Extract only URL
    def extractURL(line:String):(String) = {
       // val pattern = "\".*\"".r
        //val pattern(request:String) = line
	var url = line.split(" ")(6)
        return (url.toString)
    }

    def gettop10(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var extractedURL = accessLogs.map(extractURL)
        var urls_tuples = extractedURL.map((_,1));
        var frequencies = urls_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}
