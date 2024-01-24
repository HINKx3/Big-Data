import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("my first spark scala")
    val spark = SparkSession
      .builder()
      .appName("Hink Spark Hello World App")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("Spark Session created")
    val sampleSeq = Seq((1, "Spark"), (2, "Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")

    df.show()
    df.write.format("csv").save("first_df")
    spark.sql("CREATE TABLE IF NOT EXISTS src " +
      "(key INT, value STRING) USING hive"
    )

  }

}
