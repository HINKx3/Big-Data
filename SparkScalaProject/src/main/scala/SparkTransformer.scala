import java.util.Properties

import common.{PostgresCommon, SparkCommon}
import org.apache.spark.sql.SparkSession

object FutureXSparkTransformer {
  def main(args: Array[String]): Unit = {

    val spark : SparkSession = SparkCommon.createSparkSession()

    //Create a DataFrame from Postgres Course Catalog table
    println("Creating Dataframe from Postgres")

    val pgTable = "futureschema.futurex_course_catalog"
    //server:port/database_name
    val pgCourseDataframe = PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable)
    println("Fetched")

    pgCourseDataframe.show()
    println("Shown")

  }
}
