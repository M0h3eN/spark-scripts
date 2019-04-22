package ir.son.ix

import ir.son.ix.commons.DateManipulation.diffMinute
import ir.son.ix.commons.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.max

import scala.annotation.tailrec
import scala.util.Try

object RevaluatorDistributed {
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



    @tailrec
    def computeReturn(sellerfreq: RDD[Int], sellerprice: RDD[Double],
                      sellertime: RDD[Long], buyerfreq: RDD[Int],
                      buyerprice: RDD[Double], buyertime: RDD[Long],
                      tinput: Double = 0, nplusinput: Double = 0): Array[Double] = {

      var t: Double = tinput
      var nplus: Double = nplusinput
      // First check seller time and buyer time

/*
      sellerfreq.collect()
      sellerprice.collect()
      sellertime.collect()

      buyerfreq.collect()
      buyerprice.collect()
      buyertime.collect()

      */


      if (sellertime.first() > buyertime.first()) {
        /*
        ** Check the seller and buyer freq
        ** 1 - y_n < x_n
        ** 2 - y_n == x_n
        ** 3 - y_n > x_n
        */
        if (sellerfreq.first() < buyerfreq.first()) {

          //          println("###################: Condition 1 :###################")

          t = t + (sellerfreq.first() * (sellerprice.first() - buyerprice.first())) /
            diffMinute(sellertime.first(), buyertime.first())

          nplus = nplus + sellerfreq.first() * buyerprice.first()

          val sellerFreqNew = sellerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val sellerPriceNew = sellerprice.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val sellerTimeNew = sellertime.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)

          val buyerFreqNew = spark
            .sparkContext
            .parallelize(Seq(buyerfreq.first() - sellerfreq.first()), 1)
            .union(buyerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter})

          val buyerPriceNew = buyerprice
          val buyerTimeNew = buyertime

          if (!sellerFreqNew.isEmpty() & !buyerFreqNew.isEmpty()) {

            if (sellerTimeNew.max > buyerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

            } else if (buyerTimeNew.min < sellerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerPriceNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerTimeNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                t, nplus)

            } else {
              Array(t, nplus)
            }
          } else {
            Array(t, nplus)
          }

        } else if (sellerfreq.first().equals(buyerfreq.first())) {

          //          println("###################: Condition 2 :###################")

          t = t + (sellerfreq.first() * (sellerprice.first() - buyerprice.first())) /
            diffMinute(sellertime.first(), buyertime.first())

          nplus = nplus + sellerfreq.first() * buyerprice.first()

          val sellerFreqNew = sellerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val sellerPriceNew = sellerprice.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val sellerTimeNew = sellertime.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)

          val buyerFreqNew = buyerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val buyerPriceNew = buyerprice.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val buyerTimeNew = buyertime.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)

          if (!sellerFreqNew.isEmpty() & !buyerFreqNew.isEmpty()) {

            if (sellerTimeNew.max > buyerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

            } else if (buyerTimeNew.min < sellerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerPriceNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerTimeNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                t, nplus)

            } else {
              Array(t, nplus)
            }
          } else {
            Array(t, nplus)
          }

        } else {

          //          println("###################: Condition 3 :###################")

          t = t + (buyerfreq.first() * (sellerprice.first() - buyerprice.first())) /
            diffMinute(sellertime.first(), buyertime.first())

          nplus = nplus + buyerfreq.first() * buyerprice.first()

          val sellerFreqNew = spark
            .sparkContext
            .parallelize(Seq(sellerfreq.first() - buyerfreq.first()), 1)
            .union(sellerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter})

          val sellerPriceNew = sellerprice
          val sellerTimeNew = sellertime

          val buyerFreqNew = buyerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val buyerPriceNew = buyerprice.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)
          val buyerTimeNew = buyertime.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1)

          if (!sellerFreqNew.isEmpty() & !buyerFreqNew.isEmpty()) {

            if (sellerTimeNew.max > buyerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

            } else if (buyerTimeNew.min < sellerTimeNew.max) {

              computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
                buyerFreqNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerPriceNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                buyerTimeNew.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
                  .repartition(1),
                t, nplus)

            } else {
              Array(t, nplus)
            }
          } else {
            Array(t, nplus)
          }

        }

      } else {
        computeReturn(sellerfreq, sellerprice, sellertime,
          buyerfreq.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1),
          buyerprice.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1),
          buyertime.mapPartitionsWithIndex { (idx, iter) =>  if (idx == 0) iter.drop(1) else iter}
            .repartition(1),
          t, nplus)
      }
    }



    /*
    Read All Shs from Postgres
     */

    val shCodes = Array("آب\u200Cنـ00141  ")

    shCodes.foreach(println)
    println(s"Read ${shCodes.length} Shs:")


    /*
    Iterating over all Shs
     */

    shCodes.foreach(it => {

      println(s"######################## Working on $it Stockholder ########################")

      val shDataFrame = spark
        .read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/transactional")
        .option("dbtable", "trd.transactions")
        .option("user", "postgres")
        .option("password", "batman8941607")
        .load()
        .filter($"seller" === it || $"buyer" === it)
        .cache()

      val symbols = spark.sparkContext.broadcast( shDataFrame
        .select($"symbol")
        .collect()
        .map(_ (0).toString())
        .distinct
        .sorted
      )

//      val symbols = Array("دالبر")

      var cumulative: Array[Double] = Array(0, 0)

      /*
      Iterate Over Symbols
       */
      symbols.value.foreach(x => {

//           println(s"working on $x symbol")

        val sellerDF = shDataFrame
          .select($"symbol", $"price", $"freq", $"time")
          .filter($"symbol" === x && $"seller" === it)
          .orderBy($"time".desc)

        val sellerMaxTime = Try(sellerDF.select($"time").agg(max($"time")).head().getLong(0)).getOrElse(0)

        if (sellerMaxTime !=0) {

          val buyerDF = shDataFrame
            .select($"symbol", $"price", $"freq", $"time")
            .filter($"symbol" === x && $"buyer" === it &&
              $"time" < sellerDF.select($"time").agg(max($"time")).head().getLong(0))
            .orderBy($"time".desc)

          if(!buyerDF.take(1).isEmpty) {

            val sellerPrice = sellerDF.select($"price").rdd.map(_ (0).asInstanceOf[Double])
              .repartition(1)
            val sellerFreq = sellerDF.select($"freq").rdd.map(_ (0).asInstanceOf[Int])
              .repartition(1)
            val sellerTime = sellerDF.select($"time").rdd.map(_ (0).asInstanceOf[Long])
              .repartition(1)

            val buyerPrice = buyerDF.select($"price").rdd.map(_ (0).asInstanceOf[Double])
              .repartition(1)
            val buyerFreq = buyerDF.select($"freq").rdd.map(_ (0).asInstanceOf[Int])
              .repartition(1)
            val buyerTime = buyerDF.select($"time").rdd.map(_ (0).asInstanceOf[Long])
              .repartition(1)

            cumulative = cumulative
              .zip(computeReturn(sellerFreq, sellerPrice, sellerTime, buyerFreq, buyerPrice, buyerTime))
              .map { case (x, y) => x + y }


          }
        }

      })


      val T = cumulative.apply(0)
      val Nplus = cumulative.apply(1)
      val Ratio = T/Nplus

      println(s"Ratio: $Ratio")

    })

    spark.stop()
    spark.close()
  }
}
