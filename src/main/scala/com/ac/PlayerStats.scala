package com.ac

/**
  * @author aochong
  * @create 2019-06-08 8:18
  **/
case class PlayerStats(year: Int, name: String, position: String, age: Int, team: String,
                       GP: Int, GS: Int, MP: Double, rawStats: RawStats,
                       ZScoreStats: Option[ZScoreStats], totalZScores: Double = 0,
                       NZScoreStats: Option[ZScoreStats], totalNZScores: Double = 0)

case class RawStats(FG: Double, FGA: Double, FGP: Double, threeP: Double,
                    threePA: Double, threePP: Double, twoP: Double,
                    twoPA: Double, twoPP: Double, eFGP: Double, FT: Double,
                    FTA: Double, FTP: Double, ORB: Double, DRB: Double,
                    TRB: Double, AST: Double, STL: Double, BLK: Double,
                    TOV: Double, PF: Double, PTS: Double)

case class ZScoreStats(FGP: Double, FTP: Double, threeP: Double, TRB: Double, AST: Double,
                       STL: Double, BLK: Double, TOV: Double, PTS: Double)

object PlayerStats {
  /**
    * 将每一行数据解析成PlayerStats对象
    *
    * @param line
    * @return
    */
  def parse(line: String): Option[PlayerStats] = {
    try {
      val fields = line.replaceAll("\"", "").split(",")
      val year = fields(0).toInt
      val name = fields(2)
      val position = fields(3)
      val age = fields(4).toInt
      val team = fields(5)
      val GP = fields(6).toInt
      val GS = fields(7).toInt
      val MP = fields(8).toDouble
      val stats = fields.slice(9, 31).map(_.toDouble)
      val rawStats = RawStats(
        stats(0), stats(1), stats(2), stats(3), stats(4),
        stats(5), stats(6), stats(7), stats(8), stats(9),
        stats(10), stats(11), stats(12), stats(13), stats(14),
        stats(15), stats(16), stats(17), stats(18), stats(19),
        stats(20), stats(21))

      Some(PlayerStats(year, name, position, age, team, GP, GS, MP, rawStats, None, NZScoreStats = None))
    } catch {
      case e: NumberFormatException => {
        Console.err.println(s"error when parse : ${line}")
        None
      }
    }
  }

  /**
    * 计算7个指标的ZScore的值
    *
    * @param playerStats
    * @param aggStatsMap
    * @return
    */
  def calculatorZScore(playerStats: PlayerStats, aggStatsMap: Map[String, Double]): PlayerStats = {
    val yearStr = playerStats.year.toString
    val rawStats = playerStats.rawStats
    val fgw = (rawStats.FGP - aggStatsMap(yearStr + "_fgp_avg")) * rawStats.FGA
    val ftw = (rawStats.FTP - aggStatsMap(yearStr + "_ftp_avg")) * rawStats.FTA
    val zThreeP = (rawStats.threeP - aggStatsMap(yearStr + "_tp_avg")) / aggStatsMap(yearStr + "_tp_stddev")
    val zTRB = (rawStats.TRB - aggStatsMap(yearStr + "_trb_avg")) / aggStatsMap(yearStr + "_trb_stddev")
    val zAST = (rawStats.AST - aggStatsMap(yearStr + "_ast_avg")) / aggStatsMap(yearStr + "_ast_stddev")
    val zSTL = (rawStats.STL - aggStatsMap(yearStr + "_stl_avg")) / aggStatsMap(yearStr + "_stl_stddev")
    val zBLK = (rawStats.BLK - aggStatsMap(yearStr + "_blk_avg")) / aggStatsMap(yearStr + "_blk_stddev")
    val zTOV = (rawStats.TOV - aggStatsMap(yearStr + "_tov_avg")) / aggStatsMap(yearStr + "_tov_stddev") * (-1)
    val zPTS = (rawStats.PTS - aggStatsMap(yearStr + "_pts_avg")) / aggStatsMap(yearStr + "_pts_stddev")
    val zScoreStats = ZScoreStats(fgw, ftw, zThreeP, zTRB, zAST, zSTL, zBLK, zTOV, zPTS)
    playerStats.copy(ZScoreStats = Some(zScoreStats))
  }

  /**
    * 计算9个指标的标准化ZScore
    *
    * @param playerStats
    * @param zStatsMap
    * @return
    */
  def calculatorNZScore(playerStats: PlayerStats, zStatsMap: Map[String, Double]): PlayerStats = {
    val yearStr = playerStats.year.toString
    val zScoreStats = playerStats.ZScoreStats.get
    val zfgp = (zScoreStats.FGP - zStatsMap(yearStr + "_fgw_avg")) / zStatsMap(yearStr + "_fgw_stddev")
    val zftp = (zScoreStats.FTP - zStatsMap(yearStr + "_ftw_avg")) / zStatsMap(yearStr + "_ftw_stddev")
    val nzfgp = normaliztion(zfgp,
      (zStatsMap(yearStr + "_fgw_max") - zStatsMap(yearStr + "_fgw_avg")) / zStatsMap(yearStr + "_fgw_stddev"),
      (zStatsMap(yearStr + "_fgw_min") - zStatsMap(yearStr + "_fgw_avg")) / zStatsMap(yearStr + "_fgw_stddev"))
    val nzftp = normaliztion(zftp,
      (zStatsMap(yearStr + "_ftw_max") - zStatsMap(yearStr + "_ftw_avg")) / zStatsMap(yearStr + "_ftw_stddev"),
      (zStatsMap(yearStr + "_ftw_min") - zStatsMap(yearStr + "_ftw_avg")) / zStatsMap(yearStr + "_ftw_stddev"))
    val nzthreeP = normaliztion(zScoreStats.threeP, zStatsMap(yearStr + "_tp_max"), zStatsMap(yearStr + "_tp_min"))
    val nztrb = normaliztion(zScoreStats.TRB, zStatsMap(yearStr + "_trb_max"), zStatsMap(yearStr + "_trb_min"))
    val nzast = normaliztion(zScoreStats.AST, zStatsMap(yearStr + "_ast_max"), zStatsMap(yearStr + "_ast_min"))
    val nzstl = normaliztion(zScoreStats.STL, zStatsMap(yearStr + "_stl_max"), zStatsMap(yearStr + "_stl_min"))
    val nzblk = normaliztion(zScoreStats.BLK, zStatsMap(yearStr + "_blk_max"), zStatsMap(yearStr + "_blk_min"))
    val nztov = normaliztion(zScoreStats.TOV, zStatsMap(yearStr + "_tov_max"), zStatsMap(yearStr + "_tov_min"))
    val nzpts = normaliztion(zScoreStats.PTS, zStatsMap(yearStr + "_pts_max"), zStatsMap(yearStr + "_pts_min"))
    val statsZ = zScoreStats.copy(FGP = zfgp, FTP = zftp)
    val totalZScores = zScoreStats.productIterator.map(_.asInstanceOf[Double]).reduce(_ + _)
    val statsNZ = ZScoreStats(nzfgp,nzftp,nzthreeP,nztrb,nzast,nzstl,nzblk,nztov,nzpts)
    val totalNZScores = statsNZ.productIterator.map(_.asInstanceOf[Double]).reduce(_ + _)
    playerStats.copy(ZScoreStats = Some(statsZ), totalZScores = totalZScores,
      NZScoreStats = Some(statsNZ), totalNZScores = totalNZScores)
  }

  private def normaliztion(stats: Double, maxValue: Double, minValue: Double) = {
    import Math._
    val absMax = max(abs(maxValue), abs(minValue))
    stats / absMax
  }

}
