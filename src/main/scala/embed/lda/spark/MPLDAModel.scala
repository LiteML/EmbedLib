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
    def wc2Tm(path:String,confMap:mutable.HashMap[String,String]): Array[Double] = {
        val data=sc.textFile(s"$path/word_topic").map(_.split(" ").map(_.toInt))
        val beta = confMap("topicConcentration").toDouble
        val vocalSize=confMap("vocalSize").toDouble
        val sum = data.treeReduce((a, b)=> Array.tabulate(confMap("k").toInt)(i => a(i) + b(i)))
        val broadSum=sc.broadcast(sum)
        data.flatMap(row=>{
            val sum=broadSum.value
            Array.tabulate(confMap("k").toInt)(i=> row(i) + beta /(sum(i) + beta*vocalSize))
        }).collect()
    }

    def loadModel(path:String): LocalLDAModel = {
        val conf = sc.textFile(path+"/Configuration")
        var confMap=new mutable.HashMap[String,String]()
        conf.foreach(line=>{
            confMap.put(line.split("=")(0).trim(),line.split("=")(1).trim())
        })
        val topicMatrix= wc2Tm(path, confMap)
        new LocalLDAModel(
            Matrices.dense(confMap("vocabSize").toInt,confMap("k").toInt,topicMatrix),//topicMatrix
            Vectors.dense(confMap("docConcentration").split(",").map(_.toDouble)),//docConcentration
            confMap("topicConcentration").toDouble//topicConcentration
        )
    }
}