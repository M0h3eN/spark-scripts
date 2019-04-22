package ir.son.ix.dataIngestion

import com.datastax.driver.core.utils.UUIDs
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import scala.util.Try

object DataIngestor {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf(500, "data-ingestor")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._

    val allFiles = new ListFileJava
    val filesTr = allFiles.ListArray("172.16.11.132", "root",
      "/mnt/ConvertedFiles/AS400ConvertedFiles/TRDETL",
      "kZdjh=lka!")
    /*
    Check for last fileDate
     */

    val checker :Try[Array[Int]] = Try (spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable",  "(SELECT max(time) FROM trd.transactions) AS t")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()
      .collect
      .map(_ (0).asInstanceOf[Long].toString.substring(0, 8).toInt)
    )


    val filesToWrite = filesTr
      .toArray()
      .filter(_.asInstanceOf[String].length > 5)
      .filter(_.asInstanceOf[String].substring(0, 8).toInt > checker.getOrElse(Array(13800101)).sorted.apply(0))

    val timeUUID = udf(() => UUIDs.timeBased().toString)

    if (filesToWrite.length > 0){

      filesToWrite.foreach(x => {

        spark.read
          .format("com.springml.spark.sftp")
          .option("host", "172.16.11.132")
          .option("username", "root")
          //.option("pem", "/home/partnerpc9_ib/.ssh/id_rsa")
          .option("password", "kZdjh=lka!")
          .option("fileType", "csv")
          .option("delimiter", "|")
          .option("inferSchema", "true")
          .load(s"/mnt/ConvertedFiles/AS400ConvertedFiles/TRDETL/$x")
          .toDF()
          .filter($"TEHH" <= 13)
          .filter($"TECNCL".isNull)
          .withColumn("Time", concat($"TECENT", $"TEYY",
          when($"TEMM" < 10, concat(lit("0"), $"TEMM")).otherwise($"TEMM"),
            when($"TEDD" < 10, concat(lit("0"), $"TEDD")).otherwise($"TEDD"),
            when($"TEHH" < 10, concat(lit("0"), $"TEHH")).otherwise($"TEHH"),
            when($"TEMN" < 10, concat(lit("0"), $"TEMN")).otherwise($"TEMN"))
            .cast(LongType))
          .groupBy($"Time", $"TEYY", $"TEMM", $"TESYMB", $"TESAC", $"TEBAC")
          .agg(sum($"TESHRS").alias("Freq"), avg($"TEPRCE").alias("Price"))
          .withColumn("timeuuid", timeUUID())
          .select($"timeuuid", $"TEBAC".alias("buyer"),
            $"Freq".alias("freq"), $"Price".alias("price"),
            $"TESAC".alias("seller"), $"TESYMB".alias("symbol"),
            $"Time".alias("time"))
          .write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost/transactional")
          .option("dbtable", "trd.transactions")
          .option("user", "postgres")
          .option("password", "batman8941607")
          .mode("append")
          .save()

        println(s"$x file written")

      }
      )
    } else println("Collection is up to date.")


    spark.stop()
    spark.close()

  }

}
