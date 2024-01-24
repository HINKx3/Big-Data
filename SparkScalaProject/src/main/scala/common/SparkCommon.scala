package common

import org.apache.spark.sql.SparkSession

object SparkCommon {

  def createSparkSession(): SparkSession = {
    // Create a Spark Session
    // For Windows
    println("createSparkSession method started")
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("createSparkSession method ended")
    spark
    //println("Created Spark Session")
    //val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

    //val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
    //df.show()
    //df.write.format("csv").save("samplesq")

  }

}
