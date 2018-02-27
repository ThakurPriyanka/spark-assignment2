package edu.knoldus.operation

import org.apache.spark.sql.functions._
import edu.knoldus.util.FootBallData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Operation {

  case class FootBallSet(HomeTeam: String, AwayTeam: String, FTHG: Int, FTAG: Int, FTR: String)

  case class TeamCount(Team: String, Count: BigInt)

  case class TeamForHigh(HomeTeam: String, AwayTeam: String, FTR: String, Count: BigInt)

  case class Team(HomeTeam: String, AwayTeam: String, Count: BigInt)

  case class TotalCount(total: BigInt)

}

class Operation {

  def readCsvFile(spark: SparkSession): DataFrame = {
    FootBallData.getFootBallData(spark)
  }

  def homeTeamTotalMatch(spark: SparkSession): DataFrame = {
    val footballData = FootBallData.getFootBallData(spark)
    footballData.groupBy("HomeTeam").count()
  }

  def getTopTenWinningPercent(spark: SparkSession): DataFrame = {
    val footballData = FootBallData.getFootBallData(spark)
    val newFootBallDataCount = footballData.withColumn("Count", lit(1)).withColumn("Total", lit(1))
    val newFootballData = newFootBallDataCount.select("HomeTeam", "AwayTeam", "FTR" ,"Count", "Total")
    newFootballData.createOrReplaceTempView("viewTableFootBall")
    val homeTeamCount = spark.sql("select HomeTeam as Team, sum(Count) as Count from viewTableFootBall" +
      " where FTR = 'H' group by HomeTeam, Total")
    val AwayTeamCount = spark.sql("select AwayTeam as Team, sum(Count) as Count from viewTableFootBall" +
      " where FTR = 'A' group by AwayTeam, Total")
    val winCountRows = homeTeamCount.union(AwayTeamCount)
    winCountRows.createOrReplaceTempView("viewTableFootBallCount")

    val winCountRowsComplete = spark.sql("select Team, sum(Count) as Count " +
      "from viewTableFootBallCount group by Team")


    val totalHomeRows = spark.sql("select HomeTeam as Team ,sum(Total) as Total from viewTableFootBall group by HomeTeam")
    val totalAwayRows = spark.sql("select AwayTeam as Team, sum(Total) as Total from viewTableFootBall group by AwayTeam")

    val totalTeamRows = totalHomeRows.union(totalAwayRows)

    totalTeamRows.createOrReplaceTempView("homeAwayJoinTable")
    val countResult = spark.sql("select Team, sum(Total) as Total " +
      "from homeAwayJoinTable group by Team")

    val homeAwayJoin =  countResult.join(winCountRowsComplete, "Team")
    homeAwayJoin.createOrReplaceTempView("viewTableFootBallTotal")




    spark.udf.register("percentage",
      (count: Int, total: Int) => { if(total != 0) { (count.toDouble / total.toDouble ) * 100 } else { 0.0 }})

    val homeTeamPercentage = spark.sql("select Team, Count, Total, percentage(Count,Total) as percentage " +
      "from viewTableFootBallTotal order by percentage desc limit 10")
    homeTeamPercentage
  }

  import Operation._

  def convertDataFrameToDataSet(spark: SparkSession): Dataset[FootBallSet] = {
    val footballData = FootBallData.getFootBallData(spark)
    val newFootballData = footballData.select("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").toDF
    import spark.implicits._
    val footBallDataSet: Dataset[FootBallSet] = newFootballData.as[FootBallSet]
    footBallDataSet
  }

  def totalNumberOfTeam(spark: SparkSession): Dataset[TeamCount] = {
    val footballData = FootBallData.getFootBallData(spark)
    val footBallDataColumn = footballData.withColumn("Count", lit(1))
    val newFootballData = footBallDataColumn.select("HomeTeam", "AwayTeam", "Count").toDF
    import spark.implicits._
    val footBallDataSet: Dataset[Team] = newFootballData.as[Team]
    footBallDataSet.createOrReplaceTempView("viewTableFootBall")
    val homeTeamCount = spark.sql("select HomeTeam as Team,sum(Count) as Count from viewTableFootBall" +
      " group by HomeTeam")
    val homeDataSet: Dataset[TeamCount] = homeTeamCount.as[TeamCount]
    homeTeamCount.createOrReplaceTempView("homeCountView")
    val AwayTeamCount = spark.sql("select AwayTeam as Team ,sum(Count) as Count from viewTableFootBall" +
      " group by AwayTeam")
    val awayDataSet: Dataset[TeamCount] = AwayTeamCount.as[TeamCount]

    val homeAwayJoin = homeDataSet.union(awayDataSet)

    homeAwayJoin.createOrReplaceTempView("homeAwayJoinTable")
    val countResult = spark.sql("select Team, sum(Count) as Count " +
      "from homeAwayJoinTable group by Team")
    countResult.as[TeamCount]
  }

  def highestWinTeam(spark: SparkSession): Dataset[TeamCount] = {
    val footballData = FootBallData.getFootBallData(spark)
    val footBallDataColumn = footballData.withColumn("Count", lit(1))
    val newFootballData = footBallDataColumn.select("HomeTeam", "AwayTeam", "FTR", "Count").toDF
    import spark.implicits._
    val footBallDataSet: Dataset[TeamForHigh] = newFootballData.as[TeamForHigh]
    footBallDataSet.createOrReplaceTempView("viewTableFootBall")
    val homeTeamCount = spark.sql("select HomeTeam as Team, sum(Count) as Count from viewTableFootBall" +
      " where FTR = 'H' group by HomeTeam")
    val AwayTeamCount = spark.sql("select AwayTeam as Team, sum(Count) as Count from viewTableFootBall" +
      " where FTR = 'A' group by AwayTeam")

    val homeAwayJoin = homeTeamCount.union(AwayTeamCount)

    homeAwayJoin.createOrReplaceTempView("homeAwayJoinTable")
    val countResult = spark.sql("select Team, sum(Count) as Count " +
      "from homeAwayJoinTable group by Team order by Count desc limit 10")
    countResult.as[TeamCount]

  }
}
