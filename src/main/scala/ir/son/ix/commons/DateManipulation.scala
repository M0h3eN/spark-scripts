package ir.son.ix.commons

import java.util.Date
import com.ibm.icu.util.PersianCalendar
import com.ibm.icu.util.Calendar

/*
 Creating new PersianCalender Object
  */


object DateManipulation {

  /*
  Creating new PersianCalender Object
   */
  val persianCal = new PersianCalendar(new Date())

  /*
  Checking month Format like 02 NOT 2
   */
  def monthCheck() :String = {

    if ((persianCal.get(Calendar.MONTH) + 1) < 10)
      "0" + (persianCal.get(Calendar.MONTH) + 1).toString
    else
      (persianCal.get(Calendar.MONTH) + 1).toString

  }

  /*
  Checking Day Format like 02 NOT 2
   */
  def dayCheck() :String = {

    if (persianCal.get(Calendar.DAY_OF_MONTH) < 10)
      "0" + persianCal.get(Calendar.DAY_OF_MONTH).toString
    else
      persianCal.get(Calendar.DAY_OF_MONTH).toString

  }

  /*
  Concat Date like 13960605
   */
  def concatDate(): Int = (persianCal.get(Calendar.YEAR).toString
    + monthCheck()
    + dayCheck())
    .toInt

  /*
  Concat Date like 139606 (Year and month only)
   */

  def concatDateYearAndMonth(): Int = (persianCal.get(Calendar.YEAR).toString
    + monthCheck())
    .toInt

  /*
  Present Year difference
   */
  def currentYearDiff(col :Int) :Int = {
    persianCal.get(Calendar.YEAR) - col
  }

  /*
  Present Month difference
   */
  def currentMonthDiff(col :Int) :Int = {
    monthCheck().toInt - col
  }

  /*
  Present Day difference
   */
  def currentDayDiff(col :Int) :Int = {
    dayCheck().toInt - col
  }


  /*
  Different between to time in minute scale t1 - t2
   */
  def diffMinute(t1 :Long, t2 :Long) :Int = {
    require(t1 > t2, "t1 must be greater thann t2")

    val yeardiff = (t1.toString.substring(0,4).toInt - t2.toString.substring(0,4).toInt) * 525600
    val monthdiff = (t1.toString.substring(4,6).toInt - t2.toString.substring(4,6).toInt) * 43800
    val daydiff = (t1.toString.substring(6,8).toInt - t2.toString.substring(6,8).toInt) * 1440
    val hourdiff = (t1.toString.substring(8,10).toInt - t2.toString.substring(8,10).toInt) * 60
    val mindiff = (t1.toString.substring(10,12).toInt - t2.toString.substring(10,12).toInt) * 1

    yeardiff + monthdiff + daydiff + hourdiff + mindiff

  }


}



