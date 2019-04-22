package ir.son.ix.Analysis

import ir.son.ix.commons.DateManipulation.diffMinute
import scala.math.pow
import scala.annotation.tailrec

object RecursiveReturnEvaluator {

  val r: Double = pow(1.2, 1/(365 * 24 * 60)) - 1

  /*
  Compute the return per transaction
   */
  @tailrec
  def computeReturn(sellerfreq: Array[Int], sellerprice: Array[Double],
                    sellertime: Array[Long], buyerfreq: Array[Int],
                    buyerprice: Array[Double], buyertime: Array[Long],
                    tinput: Double = 0, nplusinput: Double = 0): Array[Double] = {

    var t: Double = tinput
    var nplus: Double = nplusinput
    // First check seller time and buyer time
    if (sellertime.max > buyertime.max) {
      /*
      ** Check the seller and buyer freq
      ** 1 - y_n < x_n
      ** 2 - y_n == x_n
      ** 3 - y_n > x_n
      */
      if (sellerfreq.head < buyerfreq.head) {

        //          println("###################: Condition 1 :###################")

        t = t + (1/pow(1 + r, diffMinute(sellertime.head, buyertime.head))) *
          (sellerfreq.head * (sellerprice.head - buyerprice.head))
        nplus = nplus + sellerfreq.head * buyerprice.head

        val sellerFreqNew = sellerfreq.drop(1)
        val sellerPriceNew = sellerprice.drop(1)
        val sellerTimeNew = sellertime.drop(1)

        val buyerFreqNew = (buyerfreq.head - sellerfreq.head) +: buyerfreq.drop(1)
        val buyerPriceNew = buyerprice
        val buyerTimeNew = buyertime

        if (sellerFreqNew.length != 0 & buyerFreqNew.length != 0) {

          if (sellerTimeNew.max > buyerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

          } else if (buyerTimeNew.min < sellerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew.drop(1), buyerPriceNew.drop(1), buyerTimeNew.drop(1), t, nplus)

          } else {
            Array(t, nplus)
          }
        } else {
          Array(t, nplus)
        }

      } else if (sellerfreq.head.equals(buyerfreq.head)) {

        //          println("###################: Condition 2 :###################")

        t = t + (1/pow(1 + r, diffMinute(sellertime.head, buyertime.head))) *
          (sellerfreq.head * (sellerprice.head - buyerprice.head))
        nplus = nplus + sellerfreq.head * buyerprice.head

        val sellerFreqNew = sellerfreq.drop(1)
        val sellerPriceNew = sellerprice.drop(1)
        val sellerTimeNew = sellertime.drop(1)

        val buyerFreqNew = buyerfreq.drop(1)
        val buyerPriceNew = buyerprice.drop(1)
        val buyerTimeNew = buyertime.drop(1)

        if (sellerFreqNew.length != 0 & buyerFreqNew.length != 0) {

          if (sellerTimeNew.max > buyerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

          } else if (buyerTimeNew.min < sellerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew.drop(1), buyerPriceNew.drop(1), buyerTimeNew.drop(1), t, nplus)

          } else {
            Array(t, nplus)
          }
        } else {
          Array(t, nplus)
        }

      } else {

        //          println("###################: Condition 3 :###################")

        t = t + (1/pow(1 + r, diffMinute(sellertime.head, buyertime.head))) *
          (buyerfreq.head * (sellerprice.head - buyerprice.head))
        nplus = nplus + buyerfreq.head * buyerprice.head

        val sellerFreqNew = (sellerfreq.head - buyerfreq.head) +: sellerfreq.drop(1)
        val sellerPriceNew = sellerprice
        val sellerTimeNew = sellertime

        val buyerFreqNew = buyerfreq.drop(1)
        val buyerPriceNew = buyerprice.drop(1)
        val buyerTimeNew = buyertime.drop(1)

        if (sellerFreqNew.length != 0 & buyerFreqNew.length != 0) {

          if (sellerTimeNew.max > buyerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew, buyerPriceNew, buyerTimeNew, t, nplus)

          } else if (buyerTimeNew.min < sellerTimeNew.max) {

            computeReturn(sellerFreqNew, sellerPriceNew, sellerTimeNew,
              buyerFreqNew.drop(1), buyerPriceNew.drop(1), buyerTimeNew.drop(1), t, nplus)

          } else {
            Array(t, nplus)
          }
        } else {
          Array(t, nplus)
        }

      }

    } else {
      computeReturn(sellerfreq, sellerprice, sellertime,
        buyerfreq.drop(1), buyerprice.drop(1), buyertime.drop(1), t, nplus)
    }
  }

}
