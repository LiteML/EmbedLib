package embed.tmp

import embed.dist.Uniform
import embed.sampling.Dice
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by takun on 02/08/2017.
  */
// Array[Int]
case class Entity(var z : Array[Int], var mcmc : Array[Array[Int]])

object Block {
  val MH = 10
  def apply(rdd:RDD[Array[Int]],k : Int, ki:Int=10) = {
    rdd.mapPartitions{
      it =>
        val dice = new Dice(k)
        val data = it.map{
          d =>
            Entity(Array.fill(ki)(dice()),Array.ofDim[Int](MH, ki))
        }.toArray
        Iterator.single(data)
    }.setName("parameter-block").persist(StorageLevel.MEMORY_AND_DISK)
  }
}
class test {

  val k = 100
  val epoch = 100
  val dice = new Dice(k)
  val uniform = new Uniform(0, 1.0)
  val MH = 10
  val βb = ?
  val β = ? // todo to specify β

  def cal_cwk(data:RDD[Array[Entity]]) = {
    //data.flatMap(_.data.map(_.z -> 1)).reduceByKey(_+_).collectAsMap()
    //<word, [<topic, count>]>
    Map.empty[Int,Map[Int,Int]]
  }

  def aliasTable(cw:Map[Int,Int]) = Array.ofDim[Int](0)
  def mh_sampling() = Array.ofDim[Int](0)
  def ? : Double = ???.asInstanceOf[Double]
  /**
    * data RDD<[(word_id, count)]>
    * @param data
    */
  def flow(data:RDD[Array[Int]]) = {
    // initialize block , and zdn
    val block = Block(data, k)
    1 to epoch foreach {
      e =>
        // by column
        var cwk = cal_cwk(block)
        val V = cwk.size
        // lw = document num of word
        data.zipPartitions(block) {
          case (it, b) =>
            val _block = b.next()
            it.foreach{
              doc => doc.indices.foreach{
                wi =>
                  val word = doc(wi)
                  val cw = cwk(word)
                  // mh samping
                  val wb = _block(wi)
                  val s = wb.z
                  1 to MH foreach {
                    i =>
                      val t = mh_sampling()
                      wb.mcmc(i) = t
                      s.indices.foreach{
                        si =>
                          val Cwt = cw(t(si))
                          val Cws = cw(s(si))
                          val Cs = ?
                          val Ct = ?
                          val π = math.min(1.0, (Cwt + β ) / (Cws + β) * (Cs + βb) / (Ct + βb))
                          if(uniform.random < π) {
                            s(i) = t(i)
                          }
                      }
                  }
                  _block(wi).z = s
              }
            }
            Iterator.single(0)
        }.count()
        cwk = null
        cwk = cal_cwk(block)
        // aliasTable
        data.zipPartitions(block) {
          case (it, b) =>
            val _block = b.next()
            it.foreach{
              doc => doc.indices.foreach {
                wi =>
                  val word = doc(wi)
                  val cw = cwk(word)
                  val urn = ? // build Alias
                  val zw = ? // draw from urn
                  1 to MH foreach {
                    i =>
                      _block(wi).z = aliasTable(cw)
                  }
              }
            }
            Iterator.single(0)
        }.count()
      // by row
    }
  }
}
