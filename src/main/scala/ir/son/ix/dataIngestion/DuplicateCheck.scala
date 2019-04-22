package ir.son.ix.dataIngestion

import ir.son.ix.commons.CSVReader.getListOfFiles
import ir.son.ix.commons.SparkConf
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object DuplicateCheck {

  def main(args: Array[String]): Unit = {
    val files = getListOfFiles("/home/partnerpc9_ib/SparkDir/SourceData/")


    println(s"File list: $files")




    /*
 Spark Configurations
   */

    val conf = SparkConf.sparkInitialConf(100, "data-checker")
    /*
     Creating Spark session
         */

    val spark = SparkConf.sparkSessionCreator(conf)


    val directory = "/home/partnerpc9_ib/SparkDir/CsvFiles/"





    /*    spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(files.head)
          .toDF()
          .coalesce(1)
          .write
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter",",")
          //.options(Map("compression" -> "zlib"))
          .mode("overwrite")
          .save(directory + files.head.substring(files.head.lastIndexOf("/") + 1))
   */



    val CollectionOne = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter",",")
      .csv(files.min)
      .toDF()


    val CollectionTwo = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter",",")
      .csv(files.max)
      .toDF()


    CollectionOne
      .persist(MEMORY_AND_DISK)

    CollectionTwo
      .persist(MEMORY_AND_DISK)


/*    val versionedDF = CollectionTwo.except(CollectionOne)*/


    println(s"DF ONE COUNT: #####################################################################" +
      s"    ${CollectionOne.dropDuplicates().count()}")

    println(s"DF TWO COUNT: #####################################################################" +
      s"    ${CollectionTwo.dropDuplicates().count()}")

/*    versionedDF
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .mode("overwrite")
      .save(directory + "Difference")
    */

    CollectionOne.unpersist()
    CollectionTwo.unpersist()

    spark.stop()
    spark.close()
  }

}
