package ir.son.ix.commons

import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlServerConnector {




  def jdbcReader(spark: SparkSession, partFlag: Boolean, partNum: Int = 100,
                 ConnStr: String, user: String, password: String,
                 PartColumn: String = "Null", lower: Int ,
                 upper: Int = DateManipulation.concatDate(),  table: String) :DataFrame = {
    /*
     Source jdbc setting ----> Sql Server
      */

    if(partFlag)
    spark
      .read
      .format("jdbc")
      .options(Map("driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url" -> s"$ConnStr",
        "user" -> s"$user",
        "password" -> s"$password",
        "partitionColumn" -> s"$PartColumn",
        "lowerBound" -> s"$lower",
        "upperBound" -> s"$upper",
        "numPartitions" -> s"$partNum",
        "dbtable" -> s"$table"))
      .load()
    else
      spark
        .read
        .format("jdbc")
        .options(Map("driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
          "url" -> s"$ConnStr",
          "user" -> s"$user",
          "password" -> s"$password",
          "dbtable" -> s"$table"))
        .load()
  }




}
