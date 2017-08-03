package embed.tmp

import embed.dist.Dice
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by takun on 02/08/2017.
  */
case class Entity(var z : Int, var mcmc : Double)
class Block(val data:Array[Entity]) extends Serializable

object Block {
  def apply(rdd:RDD[Array[(Int,Int)]],k : Int) = {
    rdd.mapPartitions{
      it =>
        val dice = new Dice(k)
        val data = it.map{
          d =>
            val z = dice().toInt
            Entity(z,0.0)
        }.toArray
        val block = new Block(data)
        Iterator.single(block)
    }.setName("parameter-block").persist(StorageLevel.MEMORY_AND_DISK)
  }
}
class test {

  val k = 100
  val epoch = 100
  val dice = new Dice(k)

  def cal_cwk(data:RDD[Block]) = {
    //data.flatMap(_.data.map(_.z -> 1)).reduceByKey(_+_).collectAsMap()
    //<word, [<topic, count>]>
    Map.empty[Int,Map[Int,Int]]
  }
  /**
    * data RDD<[(word_id, count)]>
    * @param data
    */
  def flow(data:RDD[Array[(Int,Int)]]) = {
    // initialize block , and zdn
    val block = Block(data, k)
    1 to epoch foreach {
      e =>
        var cwk = cal_cwk(block)
        // lw = document num of word
        data.zipPartitions(block) {
          case (it, b) =>
            it.foreach{
              case (word, count) =>
                // mh sampling
            }
            it
        }
    }
  }
}
