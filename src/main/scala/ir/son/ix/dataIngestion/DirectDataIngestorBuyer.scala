package ir.son.ix.dataIngestion

import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import com.datastax.driver.core.utils.UUIDs
import doobie.Transactor
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import scala.concurrent.ExecutionContext

object DirectDataIngestorBuyer {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    implicit val cs = IO.contextShift(ExecutionContext.global)

    val xa = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver", // driver classname
      "jdbc:postgresql://localhost/transactional", // connect URL (driver-specific)
      "postgres",              // user
      "batman8941607"                       // password
    )


    val shCodes = sql""" SELECT * FROM trd."nShCodes" """.query[String].to[Array].transact(xa).unsafeRunSync()


    shCodes.foreach(println)
    println(s"Length of sh codes: ${shCodes.length}")

    val timeUUID = udf(() => UUIDs.timeBased().toString)

    /*
Spark Configurations
*/

    val conf = SparkConf.sparkInitialConf(500, "data-seperator")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._


    val srcData =  spark.read
      .format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("partitionColumn", "time")
      .option("lowerBound", "138001051100")
      .option("upperBound", "139710051300")
      .option("numPartitions", 500)
      .load(s"/home/partnerpc9_ib/data/")
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
      .select($"timeuuid", $"time", $"symbol", $"buyer", $"freq", $"price")


    srcData
      .filter($"buyer".isin(shCodes: _*))
      .select($"timeuuid", $"time", $"symbol", $"buyer", $"freq", $"price")
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "trd.transactionsbuyer")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .mode("append")
      .save()

  }

}
