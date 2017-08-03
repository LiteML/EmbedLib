package embed.sampling

import java.util.ArrayDeque

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Sample Multinomial Distribution in O(1) with O(K) Build Time
  * Created by chris on 8/2/17.
  * @param wordTopicCount Array[(topic, count)]
  */
class AliasTable(val wordTopicCount:Array[(Int,Int)]) extends Serializable{
  private final val length:Int = wordTopicCount.length
  private final val alias = Array.ofDim[Int](length)
  private final val rand = Random
  private final val sum:Double = wordTopicCount.map(_._2).sum.toDouble
  private final val probability:Array[Double] = wordTopicCount.map(f => f._2/sum*length)

  def build():Unit = {
    val small = new ArrayDeque[Int]()
    val large = new ArrayDeque[Int]()
    (0 until length) foreach {i=>
      if(probability(i) < 1.0) small.add(i) else large.add(i)
    }

    while(!small.isEmpty) {
      val less = small.pop()
      val more = large.pop()
      alias(less) = more
      probability(more) -= 1.0 - probability(less)
      if( probability(more) < 1.0) small.add(more) else large.add(more)
    }
  }
  def draw():Int = {
    val column = rand.nextInt(length)
    if(rand.nextDouble() < probability(column)) wordTopicCount(column)._1 else wordTopicCount(alias(column))._1
  }
}

object AliasTable {
  def main(args: Array[String]): Unit = {
    val wordTopicCount = Array((1,100),(5,200),(3,1000),(7,400))
    val aliasTable = new AliasTable(wordTopicCount)
    aliasTable.build()
    val z = ArrayBuffer[Int]()
    (0 until 1000000) foreach {i=>
      z.append(aliasTable.draw())
    }
    println(z.count(f=>f==1))
    println(z.count(f=>f==5))
    println(z.count(f=>f==3))
    println(z.count(f=>f==7))
  }
}
