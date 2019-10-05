package ir.son.ix

import ir.son.ix.commons.SparkConf
import ir.son.ix.Analysis.RecursiveReturnEvaluator.computeReturn
import org.apache.log4j.{Level, Logger}

import scala.util.Try

object Revaluator {

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

    val shCodes = spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
      .option("dbtable", s""" (select shcode from trd.codepar where "partitionIndex" = ${args.head}) as codes """)
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()

    val writedCodes = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
      .option("dbtable", "(select shcode from trd.info) as shcode")
      .option("user", "postgres")
      .option("password", "batman8941607")
      .load()

    val requiredCodes = shCodes.except(writedCodes)
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

      val symbols = spark
        .read
        .format("jdbc")
        .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
        .option("dbtable", s"""(select symbol from trd."shSymb" where code = '$it') as b1 """)
        .option("user", "postgres")
        .option("password", "batman8941607")
        .load()
        .collect()
        .map(_ (0).toString())


      symbols.foreach(x => {

        val sellerDF =  spark
          .read
          .format("jdbc")
          .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
          .option("dbtable", s"(select time, price, freq" +
            s" from trd.transactionsseller where seller = '$it' and symbol = '$x' order by time desc) as b2 ")
          .option("user", "postgres")
          .option("password", "batman8941607")
          .load()
          .cache()

        val sellerMaxTime = Try(sellerDF.select($"time").first().getLong(0)).getOrElse(0)

        if (sellerMaxTime!= 0){

          val buyerDF = spark
            .read
            .format("jdbc")
            .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
            .option("dbtable", s"(select time, price, freq" +
              s" from trd.transactionsbuyer where buyer = '$it' and symbol = '$x' and " +
              s"time < $sellerMaxTime order by time desc) as b2 ")
            .option("user", "postgres")
            .option("password", "batman8941607")
            .load()
            .cache()

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

            buyerDF.unpersist()
            sellerDF.unpersist()
          }
          buyerDF.unpersist()
          sellerDF.unpersist()
        }
        sellerDF.unpersist()
      })

      val T = cumulative.apply(0)
      val Nplus = cumulative.apply(1)
      val Ratio = T/Nplus

      if(T != 0) {

        Seq(Sinfo(it, T, Nplus, Ratio))
          .toDF()
          .write
          .format("jdbc")
          .option("url", s"jdbc:postgresql://${args.apply(1)}/transactional")
          .option("dbtable", "trd.info")
          .option("user", "postgres")
          .option("password", "batman8941607")
          .mode("append")
          .save()

        println(s"$it Info Written")

      }

    })

    spark.stop()
    spark.close()

  }

}
