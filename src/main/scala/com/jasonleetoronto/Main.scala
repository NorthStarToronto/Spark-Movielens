package com.jasonleetoronto

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Main {

  def main(args: Array[String]): Unit = {

    /* Set the Log Level to print error only */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /* Create Spark Context/Driver utilizing local CPUs */
    val conf = new SparkConf().setMaster("local[*]").setAppName("Movie Lens")
    val sc = new SparkContext(conf)

    /* Read movies.csv into an RDD */
    val lines = sc.textFile("file:///D:/Data/ml-20m/movies.csv")

    /* Check data import status and line format */
    // lines.take(10).foreach(println)

    /* Convert .csv file to Cassandra Table friendly .tsv file*/
    // lines.take(50).map(parseLine).foreach(println)
    lines.map(parseLine)

    /* Save RDD as .tsv file */
    val results = lines.map(parseLine).coalesce(1).saveAsTextFile("file:///D:/Data/ml-20m/movies.tsv")



  }

  /* Intermediate Step for populating and connecting to Customer Transaction Data like Cassandra Table
     for distributed Spark Jobs "Clustering + BI"
     => In practice, REST API JAVA Client will be responsible for populating the Cassandra Table with the data object

     Convert original csv string line format
     from 1,Taxi Driver (1995),Comedy|Action|Thriller
     to   1 \t "Taxi Driver" \t 1995 \t [\'Comedy\',\'Action\',\'Thriller\'] "Cassandra Set Datatype Format"

     COPY ~
     FROM ~ WITH DELIMITER='\t';
   */
  def parseLine(line: String): String = {

    /* Split Line into 3X columns => 1st Column "ID", 2nd Column "Title & Year", & 3rd Column "Genre"
       Transform column 2 & 3 => Combine the columns back to string
     */

    val fields = line.replaceAll(""""""","").split(",") // remove " and split on , => Array

    /* 1st Column */
    val movieId = fields(0)

    /* 2nd Column */
    val temp = fields.slice(1, fields.size -1)

    val title = if (temp.size ==1) {
      rtrim(temp(0).split("\\(")(0))
    } else {
      temp.slice(0, temp.size -1).mkString(", ") + rtrim(temp(temp.size-1).split("\\(")(0))
    }
    val year = temp.last.split("\\(").last.replaceAll("\\W","")

    /* 3rd Column */
    val genres = fields.last.split("\\|").mkString("""[\'""","""\'\,\'""","""\']""")

    movieId+"\t"+"""""""+title+"""""""+"\t"+year+"\t"+genres
  }

  def ltrim(s: String) = s.replaceAll("^\\s+", "")
  def rtrim(s: String) = s.replaceAll("\\s+$", "")
}
