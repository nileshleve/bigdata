package com.cloudxlab.logparsing
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class IPUtils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

    def containsIP(line:String):Boolean = return line matches "^([0-9\\.]+) .*$"
    //Extract only IP
    def extractIP(line:String):(String) = {
        val pattern = "^([0-9\\.]+) .*$".r
        val pattern(ip:String) = line
        return (ip.toString)
    }

    def gettop10(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var ipaccesslogs = accessLogs.filter(containsIP)
        var cleanips = ipaccesslogs.map(extractIP(_))
        var ips_tuples = cleanips.map((_,1));
        var frequencies = ips_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}
