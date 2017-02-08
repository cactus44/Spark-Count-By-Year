/**
  * Created by guillaume on 06/02/17.
  */

import org.apache.spark.{SparkConf, SparkContext}

object FirstnameCountbyYear {

  def main(args: Array[String]) = {

    val logFile = "/home/guillaume/TP/Spark/dpt2015.txt"
    // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //pour chargement dynamique faire un SparkConf vide
    val logData = sc.textFile(logFile, 2).cache()

    val byYear = logData.filter(line => line.contains("1978"))

    val byYear1 = byYear.map(_.split("\t")).map(c => (c(1) + ";" + c(3), c(4).replace(".0000", "").toInt))
    //val result = filterFirstnameByYear.map(_.split("\t")).map(c => c(1) + "\t" + c(2) ).take(10)

    //donne le nombre de fois qu'un prénom à été utilisé par département .
    val result = byYear1.reduceByKey((a, b) => a + b).sortByKey(ascending = true)

    //sortie console pour dev
    for (x <- result.take(2000)) {
      println(s"en 1978 , il y a :$x")
    }

    //Ecriture du résultat sur disque
    result.saveAsTextFile("/home/guillaume/TP/Spark/dpt2015.txt.output")
    sc.stop()
  }
}

