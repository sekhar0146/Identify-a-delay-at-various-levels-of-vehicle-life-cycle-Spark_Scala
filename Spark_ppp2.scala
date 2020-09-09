package Assignments

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_ppp2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    // processing a.txt file
    val fileName1 = "C:/Users/z011348/Desktop/Spark/pppDataset/ppp1.txt"
    val aData = getDataWithoutHeader(fileName1)
    /*aData.collect().foreach(println)*/
    val aDF = getA(aData)
    aDF.show()
    aDF.printSchema()

    // processing b.txt file
    val fileName2 = "C:/Users/z011348/Desktop/Spark/pppDataset/ppp2.txt"
    val bData = getDataWithoutHeader(fileName2)
    /*aData.collect().foreach(println)*/
    val bDF = getB(bData)
    bDF.show()
    bDF.printSchema()

    spark.stop()

  }

  def getDataWithoutHeader(fileName : String) : RDD[String]= {
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val data = spark.sparkContext.textFile(fileName)
    val header = data.first() //extract header
    /*data.filter(row => row != header).collect().foreach(println)   //filter out header*/
    data.filter(row => row != header) //filter out header
  }

  case class layout1(col1: String, Col2: String)
  //
  def getA(aData: RDD[String]): DataFrame ={
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val DF1 = aData
      .map(a => layout1(
        a.substring(0,3),
        a.substring(3,9)
      )).toDF("col1", "col2")

    DF1
  }

  case class layout2(col1: String, Col2: String, col3: Int)
  //
  def getB(bData: RDD[String]): DataFrame ={
    val spark = SparkSession.builder()
      .appName("Spark basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val DF2 = bData
      .map(a => layout2(
        a.substring(0,3),
        a.substring(3,9),
        a.substring(9,12).toInt
      )).toDF("col1", "col2", "col3")

    DF2
  }

}