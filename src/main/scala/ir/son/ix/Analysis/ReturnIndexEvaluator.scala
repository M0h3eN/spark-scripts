package ir.son.ix.Analysis

import ir.son.ix.Analysis.RecursiveReturnEvaluator.computeReturn
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object ReturnIndexEvaluator {
  case class Sinfo(shcode: String, t: Double, nplus: Double, ratio: Double)

  def ReturnIndexEvaluatorForEachStockHolder(spark: SparkSession, sourceDataFrame: DataFrame, stockHolderCode: String) :DataFrame = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    import spark.implicits._

    var cumulative: Array[Double] = Array(0, 0)

    /*
    Iterate Over Symbols
     */

    val symbols = sourceDataFrame
      .filter($"seller" === stockHolderCode || $"buyer" === stockHolderCode)
      .select($"symbol")
      .dropDuplicates()
      .collect()
      .map(_ (0).toString())

    symbols.foreach(x => {

      val sellerDF =  sourceDataFrame
        .select($"time", $"price", $"freq")
        .filter($"seller" === stockHolderCode && $"symbol" === x)
        .orderBy($"time".desc)

      val sellerMaxTime = Try(sellerDF.select($"time").first().getLong(0)).getOrElse(0)

      if (sellerMaxTime!= 0){

        val buyerDF = sourceDataFrame
          .select($"time", $"price", $"freq")
          .filter($"buyer" === stockHolderCode && $"symbol" === x && $"time" < sellerMaxTime)
          .orderBy($"time".desc)

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

    val stockHoldersScore = Seq(Sinfo(stockHolderCode, T, Nplus, Ratio)).toDF()

    stockHoldersScore


  }

}
