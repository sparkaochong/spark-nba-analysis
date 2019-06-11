package com.ac

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author aochong
  * @create 2019-06-07 8:41
  *
spark-submit --class com.ac.PlayerStatsPreprocess \
--master spark://master:7077 \
--executor-memory 512M \
--total-executor-cores 4 \
--executor-cores 2 \
/home/aochong/example/hive-example/executable_jars/spark-nba-player-1.0-SNAPSHOT.jar \
hdfs://master:9999/user/aochong/example/hive-example/nba/basketball hdfs://master:9999/user/aochong/example/hive-example/nba/tmp

spark-submit --class com.ac.PlayerStatsPreprocess \
--master yarn \
--executor-memory 512M \
--total-executor-cores 4 \
--executor-cores 2 \
/home/aochong/example/hive-example/executable_jars/spark-nba-player-1.0-SNAPSHOT.jar \
hdfs://master:9999/user/aochong/example/hive-example/nba/basketball hdfs://master:9999/user/aochong/example/hive-example/nba/tmp

  **/
object PlayerStatsPreprocess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val configuration = new Configuration()

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }
    val spark = SparkSession.builder()
      .appName("PlayerStatsPreprocess")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val (rawDataPath, tempPath) = if (args.isEmpty) ("data/nba/basketball", "data/nba/tmp") else {
      configuration.set("fs.defaultFS", "hdfs://master:9999")
      (args(0), args(1))
    }

    Utils.deleteFileIfexists(tempPath, configuration)

    /*for(year <- 1980 to 2016){
      val yearRawData = sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*")
      yearRawData.filter(line => !line.trim.isEmpty && !line.startsWith("Rk")).map(line => {
        val temp = line.replaceAll("\\*","").replaceAll(",,",",0,")
        s"${year},${temp}"
      }).saveAsTextFile(s"${tempPath}/${year}")
    }*/

    val rdds: Seq[RDD[(Int, String)]] = (1980 to 2016).map { year =>
      sc.textFile(s"${rawDataPath}/leagues_NBA_${year}*").filter(line => !line.trim.isEmpty && !line.startsWith("Rk")).map(line => {
        val temp = line.replaceAll("\\*", "").replaceAll(",,", ",0,").replaceAll(",,", ",0,")
        (year, s"${year},${temp}")
      })
    }

    import spark.implicits._
    rdds.reduce(_.union(_)).toDF("year", "line")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year")
      .csv(tempPath)

    spark.stop()
  }
}
