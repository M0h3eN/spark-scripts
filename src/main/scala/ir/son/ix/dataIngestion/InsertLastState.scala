package ir.son.ix.dataIngestion

import ir.son.ix.commons.MongoConnector._
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

object InsertLastState {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf( appName = "InsertLastState")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)
//    val LOGGER = LoggerFactory.getLogger(this.getClass)

    val HIVE_LAST_DB_NAME = "ixdb_update"
    val HIVE_DIFF_DB_NAME = "ixdb_update_diff"
    val MONGO_DB_NAME = args.apply(0)
    val MONGO_URI = "mongodb://root:dkjHD38sdkj@mongo-mongos-1,mongo-mongos-2,mongo-mongos-3,mongo-mongos-4,mongo-mongos-5,mongo-mongos-6,mongo-mongos-7"
    val tableName = args.apply(1)

    println(s"********* Writing ${tableName + "_LAST_STATE_SHARDED"} *********")
    val SejamLast = spark.sql(s"SELECT * FROM $HIVE_DIFF_DB_NAME" + "." + tableName)

    Writer(spark, SejamLast, MONGO_URI, MONGO_DB_NAME, tableName + "_LAST_STATE_SHARDED")
    println(s"********* Finished writing ${tableName + "_LAST_STATE_SHARDED"} *********")

    spark.stop()
    spark.close()

  }

}
