package ir.son.ix.dataIngestion

import ir.son.ix.commons.CSVReader.getListOfFiles
import ir.son.ix.commons.SparkConf
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object VersionHandler {

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


/*    val df = spark.read.
      format("com.springml.spark.sftp").
      option("host", "ix.csdiran.com").
      option("username", "son").
      //option("password", "^gh67$#uJYGJ43&").
      option("pem", "/home/partnerpc9_ib/.ssh/id_rsa").
      option("fileType", "csv").
      option("delimiter", "|").
      option("inferSchema", "true").
      load("/mnt/files2/ATBLLG/13970715.csv")*/


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

    /*
    trim all spaces in all columns
     */


   val DFOne = CollectionOne.columns.foldLeft(CollectionOne) { (memoDF, colName) =>
    memoDF.withColumn(
     colName,
     regexp_replace(col(colName), "\\s+", "")
    )
   }


   val DFTwo = CollectionTwo.columns.foldLeft(CollectionTwo) { (memoDF, colName) =>
    memoDF.withColumn(
     colName,
     regexp_replace(col(colName), "\\s+", "")
    )
   }

   val versionedDF = DFTwo.except(DFOne)

//    DFOne.show()
//    DFTwo.show()


    /*
    val versionedDF = DFTwo.join(DFOne, DFOne.col("ali").eqNullSafe(DFTwo.col("ali"))
      .and(DFOne.col("javad").eqNullSafe(DFTwo.col("javad")))
      .and(DFOne.col("hamid").eqNullSafe(DFTwo.col("hamid"))), "left_anti")
*/
    println(s"COUNT: #####################################################################    ${versionedDF.count()}")


    versionedDF
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .mode("overwrite")
      .save(directory + "Difference")




    CollectionOne.unpersist()
    CollectionTwo.unpersist()



    /* for (x <- 0 until files.length - 1){

       println(files.indices)

       val CollectionOne = spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("delimiter",",")
         .csv(files.apply(x))
         .toDF()


       val CollectionTwo = spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("delimiter",",")
         .csv(files.apply(x + 1))
         .toDF()
         .drop("id")



       CollectionOne.persist(MEMORY_AND_DISK)
       CollectionTwo.persist(MEMORY_AND_DISK)

       //CollectionOne.printSchema()
       //CollectionTwo.printSchema()


      /*val versionedDF = CollectionOne
         .union(CollectionTwo)
         .dropDuplicates()
         .except(CollectionOne
           .intersect(CollectionTwo))
 */

       val columns = CollectionOne
         .columns
        // .diff(Array("SPDATE", "SPDATEKeyNotConverted"))

       /*
       val selectedFeatures = predDF
         .select(columnToSelect.head, columnToSelect.tail: _*)
       */

       val newNames = columns.map(_ + "2").toSeq
       val renamedDF = CollectionTwo.toDF(newNames: _*)


       val versionedDF = CollectionOne
         .join(renamedDF, columns.map(x => CollectionOne(x) =!= renamedDF(x + "2")).reduce(_ && _), "rightouter")
         .select(CollectionTwo.columns.head,  CollectionTwo.columns.tail:_*)
         .dropDuplicates()

       versionedDF
         .coalesce(1)
         .write
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .mode("overwrite")
         .save(directory + files.apply(x+1).substring(files.apply(x+1).lastIndexOf("/") + 1))




       println(s"Number of records: ${versionedDF.count()}, in $x iteration")


       CollectionOne.unpersist()
       CollectionTwo.unpersist()

     }*/


    spark.stop()
    spark.close()
  }
}
