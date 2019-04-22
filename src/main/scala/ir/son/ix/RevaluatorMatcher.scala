package ir.son.ix

import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}

object RevaluatorMatcher {

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
    Read All Writed Shs from Postgres
     */

    val writedCodes = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "trd.info")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()
      .filter($"ratio" > 0.00001)
      .select($"shcode")
      .collect
      .map(_ (0).toString())
      .sorted



    writedCodes.foreach(println)
    println(s"Read ${writedCodes.length} Shs:")


    /*
    Iterating over all Shs
     */

    writedCodes.foreach(it => {

      println(s"######################## Working on $it Stockholder ########################")

      spark
        .read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/transactional")
        .option("dbtable", "trd.transactions")
        .option("user", "postgres")
        .option("password", "batman8941607")
        .load()
        .filter($"seller" === it || $"buyer" === it)
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/transactional")
        .option("dbtable", "trd.collection")
        .option("user", "postgres")
        .option("password", "batman8941607")
        .mode("append")
        .save()

    })


  }
}
