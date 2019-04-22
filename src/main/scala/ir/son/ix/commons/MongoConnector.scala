package ir.son.ix.commons

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}



object MongoConnector {


  def Reader(spark: SparkSession,  uri: String, database: String, collection: String) :DataFrame = {

    val readConfig = ReadConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
    MongoSpark.load(spark, readConfig)

  }


  def Writer(spark: SparkSession, DF: DataFrame,  uri: String, database: String, collection: String) :Unit = {

    val writeConfig = WriteConfig(Map("uri" -> uri, "database" -> database, "collection" -> collection))
        MongoSpark.save(DF, writeConfig)
  }



}


