package ir.son.ix.commons

import java.io.File

object CSVReader {


  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.toString).toList.sorted
  }

  def getListOfLastFileDates(dir: String, date: Int): Int = {
    val file = new File(dir)
    val files = file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.toString).toList.sorted

    val dates = files.map(x => {
      val y = x.replaceAll("/", "__")
      val ind = y.lastIndexOf("__")
      y.slice(ind +2, ind + 10).toInt
    })

    dates.filter(_ <= date).sorted(Ordering.Int.reverse).head

  }

//  getListOfFiles("dirrectory path")


}


