














































































































































package embed.tmp

import java.util.Random

import embed.lda.LDAConfig
import embed.sampling.{AliasTable, Dice}
import org.apache.spark.rdd.RDD
import embed.lda.mh.Sampler
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chris on 8/14/17.
  */
// 假设所有数据结构是Dense的。
// 本地数据是D*V（按照文章存储，每个文章用词包抽象）, server上的数据是该模型的model(word-topic count table,
// 相当于lr里面的参数) 以及一个global topic count。纪录所有文章中每一个topic有多少词
// Array的下标就是词的id，因此D*V 实际上存储的是每一个token所分配的topic
object PsWarpLda {
  def main(args: Array[String]): Unit = {

    //迭代次数
    val iterations = 1000

    //总词数
    val word = 100

    //总topic数
    val topic = 10

    //mcmc的步数
    val mhSteps = 10

    val r = new Random()

    //初始化模型word-topic matrix 根据DICE随机生成
    val model = Initialization.topicInitialization(word,topic)

    // lda configuration
    val config = LDAConfig()

    // 注册ps word-topic count table
    val pSMatrix = PSMatrix().createModel(model)

    // 计算出每一个topic 有多少个token，数据格式是dense array，以下标来表示topic
    val globalTopicCount = model.transpose.map{f=>
      f.sum
    }

    //注册 ps global vector
    val pSVector = PSVector().createModel(globalTopicCount)

    //data这里面就是D×V的topic
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val data: RDD[Array[Int]] = sc.parallelize(model)

    //进行迭代
    (0 until iterations) foreach{i=>

      data.mapPartitions{documents =>
        //为了防止通信过多在本地建立alias table这样会产生多余的alias table，比如“我”这个词会在每个partition上出现建立一个aliastable，这些aliastable实际上是重复的，
        // 实际上是可以将建立aliastable放在ps上边的，这个需要后期优化
        val buf = new collection.mutable.ArrayBuffer[Array[Int]](documents.length)

        documents.copyToBuffer(buf)


        //检查该partition里面包含多少个word
        val wordsOfPartitions = buf.flatMap(f=>f).distinct.toArray

        //通过这些word 从ps里面将pSMatrix对应的行pull下来
        val wordsDistributions = pSMatrix.pullByRows(wordsOfPartitions).zip(wordsOfPartitions).map{

          //从psMatrix拉下来行，对每一个词建立aliastable，并记录下wordTopicDistribution
          case (topicDistribution, word) =>
            // sparse topic Distribution
            val sparseTopicDistribution = topicDistribution.zipWithIndex.filter(_._1 != 0).map(f =>(f._2,f._1))
            val aliasTable = new AliasTable(sparseTopicDistribution)
            (word,(aliasTable, topicDistribution))
        }.toMap[Int,(AliasTable,Array[Int])]

        buf.map{tokens => // tokens这里指带的是一个token的topic， token对应的词用下标表示（tokens是文章的topic集合）
          val documentTopicAssignments = tokens
          val documentSize: Int = tokens.length
          val sampler = new Sampler(config, mhSteps, r)

          //sampler需要根据每一个document定制
          //每一个 token -被assgin了一个topic
          sampler.documentTopicAssignments = documentTopicAssignments

          //每一个topic在该document里面有多少个
          sampler.documentCounts = for(i <-0 until topic) yield {i =>
            documentTopicAssignments.count(f => f == i)
          }

          //document的size
          sampler.documentSize = documentSize
          (0 until tokens.length) map{i =>
            // 从每一个partition的wordsDistribution Count table 中取出来每一个词对应的alias table 和 topic 分布
            val (aliasTable,topicDistribution) = wordsDistributions.get(i).get
            var wordCounts = topicDistribution

            //将global topic vector pull下来
            var globalCounts = pSVector.pullByRows(0)

            //sampler 需要根据词来定制
            sampler.globalCounts = globalCounts
            sampler.aliasTable = aliasTable
            sampler.wordCounts = wordCounts

            //根据上一步的结果进行采样
            val z = sampler.sample(tokens(i))

            if (z != tokens(i)){
              globalCounts(z) += 1
              globalCounts(tokens(i)) -= 1
              wordCounts(z) += 1
              wordCounts(tokens(i)) -= 1
              //更新word - topic table
              pSMatrix.incrementAndFlush(i, wordCounts)
              //更新global topic count
              pSVector.incrementAndFlush(0,globalCounts)
            }
            z //更新本地的D*V 表
        }

        }.toIterator
      }
    }
  }
}


// 存储word - topic count table
case class PSMatrix {
  def createModel(matrix:Array[Array[Int]]):PSMatrix = null
  def mkRemote(matrix:Array[Array[Int]]):Unit = Unit
  def pullByRows(rowId:Array[Int]):Array[Array[Int]] = null
  def pushByRow(rowId:Int,updateResult:Array[Int]):Unit = Unit
  def incrementAndFlush(rowId:Int, updateResult:Array[Int]):Unit = Unit
}
//存储 topic count(全体文章里面topic k有多少个词)
case class PSVector{
  def createModel(matrix:Array[Int]):PSVector = null
  def mkRemote(matrix:Array[Array[Int]]):Unit = Unit
  def pullByRows(rowId:Int):Array[Long] = null
  def pushByRow(rowId:Int,updateResult:Array[Int]):Unit = Unit
  def incrementAndFlush(rowId:Int, updateResult:Array[Long]):Unit = Unit

}



object Initialization {

  // Initialize the topic Matrix
  //w ->多少个不同的词，k -> 多少个不同的主题
  def topicInitialization(w:Int, k:Int) = {
    val dice = new Dice(k)
    Array.fill[Int](w, k)(dice.apply())
  }
}