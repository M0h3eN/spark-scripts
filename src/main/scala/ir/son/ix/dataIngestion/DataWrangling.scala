package ir.son.ix.dataIngestion

import ir.son.ix.commons.{MongoConnector, SparkConf}
import org.apache.spark.sql.functions.{abs, max, min, sum}

object DataWrangling {

  def main(args: Array[String]): Unit = {

    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf(500, "data-ingestor")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._


      val trdetlDF = MongoConnector.Reader(spark,
      "mongodb://root:hj76gh45ty@ix.csdiran.com:10000/?authSource=admin",
      "ixdb_spark4", "TRDETL")


    val sellDF = trdetlDF
      .groupBy("TESYMB", "TESYMB_Original", "TESAC_Original", "TESAC" )
      .agg(sum($"TOTAL_PRICE" ).alias("totalValue"),
        sum($"TEPRCE").alias("Price"),
        sum($"TESHRS").alias("Freq"),
        max($"TEYY").alias("Year"),
        max($"TEMM").alias("TEMM"),
        max($"TEDD").alias("TEDD"),
        max($"TEHH").alias("TEHH"),
        max($"TEMN").alias("TEMN"),
        max($"TESS").alias("TESS"))
      //.orderBy($"TESYMB", $"TESAC_Original")


    val buyDF = trdetlDF
      .groupBy("TESYMB", "TESYMB_Original", "TEBAC_Original", "TEBAC")
      .agg(sum($"TOTAL_PRICE" ).alias("totalValue"),
        sum($"TEPRCE").alias("Price"),
        sum($"TESHRS").alias("Freq"),
        min($"TEYY").alias("Year"),
        min($"TEMM").alias("TEMM"),
        min($"TEDD").alias("TEDD"),
        min($"TEHH").alias("TEHH"),
        min($"TEMN").alias("TEMN"),
        min($"TESS").alias("TESS"))
      //.orderBy($"TESYMB", $"TEBAC")


    val profDF = buyDF
      .join(sellDF,
        buyDF.col("TESYMB_Original") === sellDF.col("TESYMB_Original") &&
        buyDF.col("Freq") ===  sellDF.col("Freq") &&
        $"TEBAC_Original" === $"TESAC_Original")
      .select(buyDF.col("TESYMB_Original").alias("TESYMB"),
        buyDF.col("TEBAC_Original").alias("CODE_Original"),
        buyDF.col("TEBAC").alias("CODE"),
        buyDF.col("Year").alias("buyYear"),
        buyDF.col("TEMM").alias("buyMonth"),
        buyDF.col("TEDD").alias("buyDay"),
        buyDF.col("TEHH").alias("buyHour"),
        buyDF.col("TEMN").alias("buyMinute"),
        buyDF.col("TESS").alias("buySecond"),
        buyDF.col("Freq").alias("buyFreq"),
        buyDF.col("totalValue").alias("buyTotalValue"),
      sellDF.col("Year").alias("sellYear"),
        sellDF.col("TEMM").alias("sellMonth"),
        sellDF.col("TEDD").alias("sellDay"),
        sellDF.col("TEHH").alias("sellHour"),
        sellDF.col("TEMN").alias("sellMinute"),
        sellDF.col("TESS").alias("sellSecond"),
        sellDF.col("Freq").alias("sellFreq"),
        sellDF.col("totalValue").alias("sellTotalValue"))


    val detaideldDF = profDF
        .withColumn("SecondPerEachTrade", abs($"sellYear" - $"buyYear") * 3.154E+7 +
          abs($"sellMonth" - $"buyMonth") * 2.628E+6 +
          abs($"sellDay" - $"buyDay") * 86399.90531424 +
          abs($"sellHour" - $"buyHour") * 3599.996054759999879 +
          abs($"sellMinute" - $"buyMinute") * 59.999934245999995142 +
          abs($"sellSecond" - $"buySecond"))
        .withColumn("returnRatio", ($"sellTotalValue" - $"buyTotalValue") / $"buyTotalValue")
        .withColumn("returnRatioPercentPerSecond", ($"returnRatio"/$"SecondPerEachTrade") * 100)

    val aggregatedDF = detaideldDF
      .groupBy($"CODE_Original")
      .agg(sum($"returnRatioPercentPerSecond").alias("returnPercentPersecond"),
        (sum($"returnRatioPercentPerSecond") * 60 ).alias("returnPercentPerMinute") ,
        (sum($"returnRatioPercentPerSecond") * 3599.996054759999879).alias("returnPercentPerHour"),
        (sum($"returnRatioPercentPerSecond") * 86399.905314239993459).alias("returnPercentPerDay"))
      .orderBy( $"returnPercentPersecond".desc)

    aggregatedDF
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .mode("overwrite")
      .save("/home/partnerpc9_ib/SparkDir/CsvFiles/aggregated")

    spark.stop()
    spark.close()
  }



}
