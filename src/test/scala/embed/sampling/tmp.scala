package embed.sampling

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by takun on 02/08/2017.
  */
object tmp {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("t").setMaster("local"))
    val dice = new Dice(100)
    val b = sc.parallelize(1 to 100, 4).map{
      i => i -> dice()
    }
    val block = b.mapPartitionsWithIndex{
      (idx, it) =>
        Iterator.single(Array(0 -> 0.0))
    }.setName("block").persist(StorageLevel.MEMORY_AND_DISK)
    1 to 10 foreach{
      i =>
        b.zipPartitions(block) {
          (it1, it2) =>
            val a = it2.next()
            a(0) = (a(0)._1 + 1) -> (a(0)._2 + it1.map(_._2).sum)
            Iterator.single(0)
        }.count
    }
    block.foreach(a => println(a(0)))
  }
}
