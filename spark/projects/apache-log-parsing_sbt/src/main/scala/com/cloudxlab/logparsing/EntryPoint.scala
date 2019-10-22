package com.cloudxlab.logparsing
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
object EntryPoint {
    val usage = """
        Usage: EntryPoint <Integer (0 for Top 10 IP, 1 for Top 10 URLs, 2 for Top 10 HighTrafficTimes, 3 for Top 10 LowTrafficTimes and 4 for All Http Response Code Frequencies)> <how_many eg 5/10 or -1 for all> <file_or_directory_in_hdfs>
        Eample: EntryPoint 1 10 /data/spark/project/access/access.log.45.gz
                EntryPoint 2 10 /data/spark/project/access/access.log.45.gz
                EntryPoint 3 10 /data/spark/project/access/access.log.45.gz
                EntryPoint 4 -1 /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 4) {
            println("Expected:4 , Provided: " + args.length)
            println(usage)
            return;
        }
        var problemNumber:Int = args(1).toInt
        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("ScalaProject")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")
        
        if(problemNumber == 0){
            var utils = new IPUtils
            // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
            var accessLogs = sc.textFile(args(3))
        
            val top10 = utils.gettop10(accessLogs, sc, args(2).toInt)
            println("===== TOP 10 IP Addresses =====")
            for(i <- top10){
                println(i)
            }
        }
        else if (problemNumber == 1){
            var utils = new URLUtils
            // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
            var accessLogs = sc.textFile(args(3))
            val top10 = utils.gettop10(accessLogs, sc, args(2).toInt)
            println("===== TOP 10 URLs =====")
            for(i <- top10){
                println(i)
            }
        }
        else if (problemNumber == 2){
            var utils = new HighTrafficTimeUtils
            // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
            var accessLogs = sc.textFile(args(3))
            val top5 = utils.gettop5(accessLogs, sc, args(2).toInt)
            println("===== TOP 5 HighTrafficTimes =====")
            for(i <- top5){
                println(i)
            }
        }
        else if (problemNumber == 3){
            var utils = new LowTrafficTimeUtils
            // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
            var accessLogs = sc.textFile(args(3))
            val bottom5 = utils.gettop5(accessLogs, sc, args(2).toInt)
            println("===== TOP 5 LowTrafficTimes =====")
            for(i <- bottom5){
                println(i)
            }
        }
        else if (problemNumber == 4){
            var utils = new HttpUtils
            // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
            var accessLogs = sc.textFile(args(3))
            val uniqueHttp = utils.getUniqueHttp(accessLogs, sc)
            println("===== All HTTP Response Code Frequencies =====")
            for(i <- uniqueHttp){
                println(i)
            }
        }
    }
}
