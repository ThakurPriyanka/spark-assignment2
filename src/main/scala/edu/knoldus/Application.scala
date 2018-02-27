package edu.knoldus

import edu.knoldus.operation.Operation
import edu.knoldus.util.SparkConfigrationFile
import org.apache.log4j.{Level, Logger}


object Application {
  def main(args: Array[String]): Unit ={
//    Logger.getLogger("org".setLevel(Level.OFF))
    val log = Logger.getLogger(this.getClass)

    val operationObj = new Operation
    val spark = SparkConfigrationFile.getSparkSession()
//    log.info(s"${operationObj.readCsvFile(spark).show()}")
//    log.info(s"${operationObj.homeTeamTotalMatch(spark).show}")
    log.info(s"${operationObj.getTopTenWinningPercent(spark).show}")
//    log.info(s"${operationObj.convertDataFrameToDataSet(spark).show}")
//    log.info(s"${operationObj.totalNumberOfTeam(spark).show}")
//    log.info(s"${operationObj.highestWinTeam(spark).show}")
  }

}
