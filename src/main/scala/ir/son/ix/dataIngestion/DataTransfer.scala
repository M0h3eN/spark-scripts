package ir.son.ix.dataIngestion

import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import doobie.Transactor
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

import scala.concurrent.ExecutionContext

object DataTransfer {

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


    /*
 Spark Configurations
   */
    val conf = SparkConf.sparkInitialConf(700, "data-transfer")


    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._


    val srcData = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "trd.transactions")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .option("partitionColumn", "time")
      .option("lowerBound", "138001051100")
      .option("upperBound", "139710051300")
      .option("numPartitions", 500)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    srcData
      .filter($"seller".isin(shCodes: _*))
      .select($"timeuuid", $"time", $"symbol", $"seller", $"freq", $"price")
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "trd.transactionsseller")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .mode("append")
      .save()

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
