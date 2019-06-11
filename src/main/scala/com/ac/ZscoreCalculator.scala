package com.ac

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
  * @author aochong
  * @create 2019-06-08 9:18
export HADOOP_CONF_DIR=/home/aochong/bigdata/hadoop-2.7.7/etc/hadoop
spark-submit --class com.ac.ZscoreCalculator \
--master yarn \
--executor-memory 512M \
--num-executors 2 \
--executor-cores 2 \
/home/aochong/example/hive-example/executable_jars/spark-nba-player-1.0-SNAPSHOT.jar \
hdfs://master:9999/user/aochong/example/hive-example/nba/tmp nba

  **/
object ZscoreCalculator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("ZscoreCalculator")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val (yearRawDataPath,db) = if (args.isEmpty) ("data/nba/tmp","default") else (args(0),args(1))

    //1. 计算每一年每一个指标的平均数和标准方差
    // 1.1 解析预处理之后的csv文件，生成playerStats的DataSet
    import spark.implicits._
    val yearRawStats = sc.textFile(s"${yearRawDataPath}/*/*")
    val parsedPlayerStats: Dataset[PlayerStats] = yearRawStats.flatMap(PlayerStats.parse(_)).toDS()
    parsedPlayerStats.cache()

    // 1.2 计算每年9个指标的平均值和标准方差
    //    parsedPlayerStats.printSchema()
    import org.apache.spark.sql.functions._
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val aggStats: DataFrame = parsedPlayerStats.select($"year", $"rawStats.FGP".as("fgp"), $"rawStats.FTP".as("ftp"),
      $"rawStats.threeP".as("tp"), $"rawStats.TRB".as("trb"), $"rawStats.AST".as("ast"),
      $"rawStats.STL".as("stl"), $"rawStats.BLK".as("blk"), $"rawStats.TOV".as("tov"),
      $"rawStats.PTS".as("pts"))
      .groupBy($"year")
      .agg(
        avg($"fgp").as("fgp_avg"), avg($"ftp").as("ftp_avg"),
        avg($"tp").as("tp_avg"), avg($"trb").as("trb_avg"),
        avg($"ast").as("ast_avg"), avg($"stl").as("stl_avg"),
        avg($"blk").as("blk_avg"), avg($"tov").as("tov_avg"),
        avg($"pts").as("pts_avg"), stddev($"tp").as("tp_stddev"),
        stddev($"trb").as("trb_stddev"), stddev($"ast").as("ast_stddev"),
        stddev($"stl").as("stl_stddev"), stddev($"blk").as("blk_stddev"),
        stddev($"tov").as("tov_stddev"), stddev($"pts").as("pts_stddev"))

    def row2Map(statsDF: DataFrame) = {
      statsDF.collect().map { row =>
        val year = row.getAs[Int]("year")
        val valueMap = row.getValuesMap[Double](row.schema.map(_.name).filterNot(_.equals("year")))
        valueMap.map { case (key, value) => (s"${year}_${key}", value) }
      }.reduce(_ ++ _)
    }

    val aggStatsMap: Map[String, Double] = row2Map(aggStats)
    //2. 计算每一个球员的每年每个指标的zscore以及zscore的总值
    val aggStatsMapBroadcast = sc.broadcast(aggStatsMap)

    val statsWithZScore: Dataset[PlayerStats] = parsedPlayerStats.map(PlayerStats.calculatorZScore(_, aggStatsMapBroadcast.value))
    //    statsWithZScore.printSchema()
    //    statsWithZScore.show(false)

    //3. 计算标准化的zscore
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val zStats = statsWithZScore.select($"year", $"ZScoreStats.FGP".as("fgp"), $"ZScoreStats.FTP".as("ftp"),
      $"ZScoreStats.threeP".as("tp"), $"ZScoreStats.TRB".as("trb"), $"ZScoreStats.AST".as("ast"),
      $"ZScoreStats.STL".as("stl"), $"ZScoreStats.BLK".as("blk"), $"ZScoreStats.TOV".as("tov"),
      $"ZScoreStats.PTS".as("pts"))
      .groupBy("year")
      .agg(avg($"fgp").as("fgw_avg"), avg($"ftp").as("ftw_avg"),
        stddev($"fgp").as("fgw_stddev"), stddev($"ftp").as("ftw_stddev"),
        min($"fgp").as("fgw_min"), min($"ftp").as("ftw_min"),
        max($"fgp").as("fgw_max"), max($"ftp").as("ftw_max"),
        min($"tp").as("tp_min"), min($"trb").as("trb_min"),
        min($"ast").as("ast_min"), min($"blk").as("blk_min"),
        min($"stl").as("stl_min"), min($"tov").as("tov_min"),
        min($"pts").as("pts_min"), max($"tp").as("tp_max"),
        max($"trb").as("trb_max"), max($"ast").as("ast_max"),
        max($"blk").as("blk_max"), max($"stl").as("stl_max"),
        max($"tov").as("tov_max"), max($"pts").as("pts_max"))

    val zStatsMap: Map[String, Double] = row2Map(zStats)

    val zStatsBroadcast = sc.broadcast(zStatsMap)

    val statsWithNZScore = statsWithZScore.map(PlayerStats.calculatorNZScore(_, zStatsBroadcast.value))
    //    statsWithNZScore.printSchema()
    //    statsWithNZScore.show(false)

    // 4. 计算每一个球员的每年的经验值(等于球员从事篮球比赛的年份)
    //4.1：将schema打平，可以用schema + RDD[Row]
    val schemaN = StructType(
      StructField("name", StringType, true) ::
        StructField("year", IntegerType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("position", StringType, true) ::
        StructField("team", StringType, true) ::
        StructField("GP", IntegerType, true) ::
        StructField("GS", IntegerType, true) ::
        StructField("MP", DoubleType, true) ::
        StructField("FG", DoubleType, true) ::
        StructField("FGA", DoubleType, true) ::
        StructField("FGP", DoubleType, true) ::
        StructField("3P", DoubleType, true) ::
        StructField("3PA", DoubleType, true) ::
        StructField("3PP", DoubleType, true) ::
        StructField("2P", DoubleType, true) ::
        StructField("2PA", DoubleType, true) ::
        StructField("2PP", DoubleType, true) ::
        StructField("eFG", DoubleType, true) ::
        StructField("FT", DoubleType, true) ::
        StructField("FTA", DoubleType, true) ::
        StructField("FTP", DoubleType, true) ::
        StructField("ORB", DoubleType, true) ::
        StructField("DRB", DoubleType, true) ::
        StructField("TRB", DoubleType, true) ::
        StructField("AST", DoubleType, true) ::
        StructField("STL", DoubleType, true) ::
        StructField("BLK", DoubleType, true) ::
        StructField("TOV", DoubleType, true) ::
        StructField("PF", DoubleType, true) ::
        StructField("PTS", DoubleType, true) ::
        StructField("zFG", DoubleType, true) ::
        StructField("zFT", DoubleType, true) ::
        StructField("z3P", DoubleType, true) ::
        StructField("zTRB", DoubleType, true) ::
        StructField("zAST", DoubleType, true) ::
        StructField("zSTL", DoubleType, true) ::
        StructField("zBLK", DoubleType, true) ::
        StructField("zTOV", DoubleType, true) ::
        StructField("zPTS", DoubleType, true) ::
        StructField("zTOT", DoubleType, true) ::
        StructField("nFG", DoubleType, true) ::
        StructField("nFT", DoubleType, true) ::
        StructField("n3P", DoubleType, true) ::
        StructField("nTRB", DoubleType, true) ::
        StructField("nAST", DoubleType, true) ::
        StructField("nSTL", DoubleType, true) ::
        StructField("nBLK", DoubleType, true) ::
        StructField("nTOV", DoubleType, true) ::
        StructField("nPTS", DoubleType, true) ::
        StructField("nTOT", DoubleType, true) :: Nil
    )

    val playerRDD = statsWithNZScore.rdd.map{ player =>
      Row.fromSeq(Array(player.name, player.year, player.age, player.position,
        player.team,player.GP,player.GS, player.MP)
      ++ player.rawStats.productIterator.map(_.asInstanceOf[Double])
      ++ player.ZScoreStats.get.productIterator.map(_.asInstanceOf[Double])++ Array(player.totalZScores)
      ++ player.NZScoreStats.get.productIterator.map(_.asInstanceOf[Double]) ++ Array(player.totalNZScores))
    }

    val playerDF = spark.createDataFrame(playerRDD,schemaN)
    //4.2 求每个球员的经验值
    playerDF.createOrReplaceTempView("player")
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val playerStatsZ: DataFrame = spark.sql("select (p.age - t.min_age) as experience,p.* from player as p " +
      "join (select name, min(age) as min_age from player group by name) as t on p.name = t.name")

    //5. 结果写入本地文件中
//    playerStatsZ.write.mode(SaveMode.Overwrite).csv("data/nba/playerStatsZ")
    playerStatsZ.write.mode(SaveMode.Overwrite).saveAsTable(s"${db}.player")

    spark.stop()
  }
}
