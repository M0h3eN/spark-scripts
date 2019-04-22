package ir.son.ix.dataIngestion

import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf

import scala.util.Random

object RandomNumberGenerator {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf(300, "R-Computer")
    /*
     Create Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._

    /*
    Read All Shs from Postgres
     */

    val partitionIndex = udf(() => Random.nextInt(3))

    spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", s""" (select shcode from trd."nShCodes") as codes """)
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()
      .withColumn("partitionIndex", partitionIndex())
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "trd.codePar")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .mode("append")
      .save()
  }
}
