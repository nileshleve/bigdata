package com.cloudxlab.logparsing
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
class HttpUtils extends Serializable {
    
    def containsHttp(line:String):Boolean = return line.split(" ").length == 10
    //Extract only Http response codes
    def extractHttp(line:String):(String) = {
        var httpCode = line.split(" ")(8)
        return (httpCode.toString)
    }
    def getUniqueHttp(accessLogs:RDD[String], sc:SparkContext):RDD[(String,Int)] = {
        //Keep only the lines which have IP
        var filteredLines = accessLogs.filter(containsHttp)
        var extractedHttp = filteredLines.map(extractHttp)
        var http_tuples = extractedHttp.map((_,1));
        var frequencies = http_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies
    }
}
