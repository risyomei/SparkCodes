import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDFStructJoin {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark Paired")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val df = Seq((1)).toDF("seq")
    val masterDF = spark.read.option("header", false).csv("hdfs://dev01/tmp/dict.txt").toDF("key", "value")
    masterDF.repartition(64)
    val cacheDF = masterDF.persist()

    var targetDF = cacheDF.sample(false,0.8, 7).limit(56*60*10)
    cacheDF.createOrReplaceTempView("master")
    targetDF.createOrReplaceTempView("tgt_keys")

    println("Master Dictionary Record Count: " + cacheDF.count())
    println("Target Record Count: " + targetDF.count)

    df.withColumn("current_timestamp", current_timestamp().as("current_timestamp")).show(false)
    spark.sql("select master.key,master.value from master,tgt_keys where master.key = tgt_keys.key").show(56*60*10)
    df.withColumn("current_timestamp", current_timestamp().as("current_timestamp")).show(false)
    println("Complete")
  }
}


