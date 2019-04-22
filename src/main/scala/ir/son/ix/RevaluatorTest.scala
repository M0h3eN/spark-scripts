package ir.son.ix

import ir.son.ix.commons.SparkConf
import ir.son.ix.Analysis.RecursiveReturnEvaluator.computeReturn
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

import scala.util.Try

object RevaluatorTest {

  case class Sinfo(shcode: String, t: Double, nplus: Double, ratio: Double)

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

    val requiredCodes = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/transactional")
      .option("dbtable", "(select shcode from trd.info) as shcode")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()
      .collect
      .map(_ (0).toString())
      .sorted


    requiredCodes.foreach(println)
    println(s"Read ${requiredCodes.length} Shs:")


    /*
    Iterating over all Shs
     */

    requiredCodes.foreach(it => {

      println(s"######################## Working on $it Stockholder ########################")

      var cumulative: Array[Double] = Array(0, 0)

      /*
      Iterate Over Symbols
       */
      spark
        .read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/transactional")
        .option("dbtable", s"(select DISTINCT symbol from trd.transactions where seller = '$it' or buyer = '$it') as b1")
        .option("user", "postgres")
        .option("password", "batman8941607")
        .load()
        .collect()
        .map(_ (0).toString())
        .sorted
        .foreach(x => {

          val sellerDF =  spark
            .read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost/transactional")
            .option("dbtable", s"(select time, price, freq" +
              s" from trd.transactions where seller = '$it' and symbol = '$x' order by time desc) as b2 ")
            .option("user", "postgres")
            .option("password", "batman8941607")
            .load()

          /*shDataFrame
          .select($"symbol", $"price", $"freq", $"time")
          .filter($"symbol" === x && $"seller" === it)
          .orderBy($"time".desc)*/

          val sellerMaxTime = Try(sellerDF.select($"time").first().getLong(0)).getOrElse(0)

          if (sellerMaxTime!= 0) {

            val buyerDF = spark
              .read
              .format("jdbc")
              .option("url", "jdbc:postgresql://localhost/transactional")
              .option("dbtable", s"(select time, price, freq" +
                s" from trd.transactions where buyer = '$it' and symbol = '$x' and " +
                s"time < $sellerMaxTime order by time desc) as b2 ")
              .option("user", "postgres")
              .option("password", "batman8941607")
              .load()

            /*   shDataFrame
              .select($"symbol", $"price", $"freq", $"time")
              .filter($"symbol" === x && $"buyer" === it &&
                $"time" <= sellerDF.select($"time").agg(max($"time")).head().getLong(0))
              .orderBy($"time".desc)*/

            if(!buyerDF.take(1).isEmpty) {

              val sellerPrice = sellerDF.select($"price")
                .collect.map(_ (0).asInstanceOf[Double])
              val sellerFreq = sellerDF.select($"freq")
                .collect.map(_ (0).asInstanceOf[Int])
              val sellerTime = sellerDF.select($"time")
                .collect.map(_ (0).asInstanceOf[Long])

              val buyerPrice = buyerDF.select($"price")
                .collect.map(_ (0).asInstanceOf[Double])
              val buyerFreq = buyerDF.select($"freq")
                .collect.map(_ (0).asInstanceOf[Int])
              val buyerTime = buyerDF.select($"time")
                .collect.map(_ (0).asInstanceOf[Long])

              cumulative = cumulative
                .zip(computeReturn(sellerFreq, sellerPrice, sellerTime, buyerFreq, buyerPrice, buyerTime))
                .map { case (x, y) => x + y }

            }

          }

        })

      val T = cumulative.apply(0)
      val Nplus = cumulative.apply(1)
      val Ratio = T/Nplus

      if(T != 0) {

        Seq(Sinfo(it, T, Nplus, Ratio))
          .toDF()
          .write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost/transactional")
          .option("dbtable", "trd.info2")
          .option("user", "postgres")
          .option("password", "batman8941607")
          .mode("append")
          .save()

        println(s"$it Info Written")

      } //else println(s"$it has not info")


    })

    spark.stop()
    spark.close()

  }

}
