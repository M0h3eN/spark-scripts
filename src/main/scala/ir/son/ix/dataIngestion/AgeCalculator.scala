package ir.son.ix.dataIngestion

import ir.son.ix.commons.MongoConnector._
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}

object AgeCalculator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf( appName = "AgeCalculator")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
    import spark.implicits._


    val AgeCalc = new ageUDF


    val MONGO_DB_NAME = "ixdb"
    val MONGO_URI = "mongodb://root:dkjHD38sdkj@mongo-mongos:27017"


    val trade = Reader(spark, MONGO_URI, MONGO_DB_NAME, "TRDETL")


    val tradeDF = trade
      .filter($"FILE_DATE".between(13971104, 13971207))
      .withColumn("j0_AGE", AgeCalc.calculateAge($"j0_CICDAT", $"j0_CIBORN"))
      .withColumn("j1_AGE", AgeCalc.calculateAge($"j1_CICDAT", $"j1_CIBORN"))

    println("********* Writing TRDETL *********")
    Writer(spark, tradeDF, MONGO_URI, MONGO_DB_NAME, "TRDETL")

    spark.stop()
    spark.close()

  }


}
