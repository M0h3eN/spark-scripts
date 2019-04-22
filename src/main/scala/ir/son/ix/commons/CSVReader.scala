package ir.son.ix.commons

import java.io.File

object CSVReader {


  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.toString).toList.sorted
  }

//  getListOfFiles("dirrectory path")


}


