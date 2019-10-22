package com.cloudxlab.logparsing
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class HighTrafficTimeUtils extends Serializable {

    //Extract only URL
    def extractURL(line:String):(String) = {
        //val pattern = "\".*\"".r
        //val pattern(request:String) = line
        var dateStr = line.split(" ")(3)
        var dateTime = dateStr.slice(1,15)
        return (dateTime.toString)
    }

    def gettop5(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var extractedTime = accessLogs.map(extractURL)
        var time_tuples = extractedTime.map((_,1));
        var frequencies = time_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}
