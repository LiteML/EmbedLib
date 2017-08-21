package embed.lda.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}

import scala.collection.mutable

/**
  * Created by chris on 8/18/17.
  */
object MPLDAModel {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    //load the word count matrix to topic matrix
    def wc2Tm(path:String): Array[Double] = ???
    def loadModel(path:String): LocalLDAModel = {
        val conf = sc.textFile(path+"/Configuration")
        var confMap=new mutable.HashMap[String,String]()
        conf.foreach(line=>{
            confMap.put(line.split("=")(0).trim(),line.split("=")(1).trim())
        })
        val topicMatrix= wc2Tm(path)
        new LocalLDAModel(
            Matrices.dense(confMap("vocabSize").toInt,confMap("k").toInt,topicMatrix),//topicMatrix
            Vectors.dense(confMap.get("docConcentration").toString.split(",").map(_.toDouble)),//docConcentration
            confMap("topicConcentration").toDouble//topicConcentration
        )
    }
}