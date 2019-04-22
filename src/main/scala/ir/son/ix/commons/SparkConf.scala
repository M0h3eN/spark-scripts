package ir.son.ix.commons

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConf {

  /*
      Spark Configurations
        */
  def sparkInitialConf(sqlShufflePar: Int = 1000, appName: String) :SparkConf  = {

    new SparkConf()
    .setAppName(s"$appName")
    .setMaster("local[*]")
    .set("spark.sql.shuffle.partitions",s"$sqlShufflePar")
    .set("spark.eventLog.enabled","false")

  //  .set("spark.eventLog.dir","/home/partnerpc9_ib/SparkDir/SparkEvent")
  //  .set("spark.history.fs.logDirectory","/home/partnerpc9_ib/SparkLogs")

    .set("spark.driver.memory","10g")
    .set("spark.executor-memory","14g")
    .set("spark.local.dir ","/home/partnerpc9_ib/SparkTmpDir")

    //.set("spark.memory.offHeap.enable", "true")
    // .set("spark.memory.offHeap.size", "5g")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryoserializer.buffer","64m")
//    .set("spark.kryoserializer.buffer.max","512m")
//    .set("spark.cassandra.connection.host", "localhost")
//    .set("spark.cassandra.auth.username", "")
//    .set("spark.cassandra.auth.password", "")

  }

  /*
  Spark Session
   */

  def sparkSessionCreator(conf: SparkConf) :SparkSession = {

   SparkSession
    .builder
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  }


}
