package common

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgresCommon {

  def getPostgresCommonProps() : Properties = {
    println("getPostgresCommonProps method started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","admin")
    println("getPostgresCommonProps method ended")
    pgConnectionProperties
  }
  def getPostgresServerDatabase() : String = {
    val pgURL = "jdbc:postgresql://localhost:5432/futurex"
    pgURL
  }


  def fetchDataFrameFromPgTable(spark : SparkSession, pgTable : String) : DataFrame ={
    print("fetchDataFrameFromPgTable method started")
    val pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://localhost:5432/futurex",pgTable,getPostgresCommonProps())
    print("fetchDataFrameFromPgTable method ended")
    pgCourseDataframe


  }

}
