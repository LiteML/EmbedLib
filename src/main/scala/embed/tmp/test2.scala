package embed.tmp

import embed.dist.Uniform
import embed.sampling.{AliasTable, Dice}
import org.apache.spark.rdd.RDD

/**
  * Created by chris on 8/7/17.
  */

/**
  * Pseudo Code for training
  * @param z
  * @param mcmc
  */

case class Entity2(var z : Array[Int], var mcmc : Array[Array[Int]]) {
  def indicies:Int = z.length
  def apply(i:Int) = (z(i), mcmc(i))
}

object Block2 {
  val MH = 10

  //RDD is the sparse representation for paper
  // params Array[(word, count)]

  def apply(rdd: RDD[Array[(Int,Int)]], k: Int) = {
    rdd.mapPartitions{
      it =>
        val dice = new Dice(k)
        it.map{
          doc => doc.map{
            case(word,count) =>
              word -> Entity2(Array.fill(count)(dice()), Array.fill(MH,count)(dice()))
          }
        }
    }
  }
}

class aliasTable(val input: Map[Int,Int]){
  private val probability = Array[Float]()
  def build() = 0
  def apply() = 0


}

class test2 {
  // # of topics
  val k = 100

  // # of iteration
  val epoch = 100

  // the Dice for sample topic
  val dice = new Dice(k)

  // for acceptance rate
  val uniform = new Uniform(0, 1.0)



  def ? : Double = ???.asInstanceOf[Double]
  val MH = 10
  val βb = ?
  val β = ? // todo to specify β
  val α = ?
  val αC = Array[Double]()
  val αS = αC.sum



  def cal_cwk2(data:RDD[Array[(Int,Entity2)]]) = {
    //data.flatMap(_.data.map(_.z -> 1)).reduceByKey(_+_).collectAsMap()
    //<word, [<topic, count>]>
    // Array
    Map.empty[Int,Map[Int,Int]]
  }

  def cal_ck2(cwk:Map[Int,Map[Int,Int]]) ={
    Map.empty[Int,Int]
  }

  def cal_cd(entity:Array[Entity2]) = {
    Map.empty[Int,Int]
  }

  def update_zd(mcmc:Array[Int], length:Int) = {
    val dice = new Dice(length)
     if(uniform.random < (length/(length + αS))) {
      mcmc(dice())
    } else {
      dice()
    }
  }

  def flow2(data:RDD[Array[(Int,Int)]]) = {
    var block = Block2(data, k)
    1 to epoch foreach {
      e =>
        var cwk = cal_cwk2(block)
        var c = cal_ck2(cwk)
        val V = cwk.size
        val byColumn = block.map{doc =>
          doc.map{case(word, entity)=>
            val cw = cwk(word)
            val topic_indicator = (0 to entity.indicies) map { i =>
              var s, t = 0
              var z = 0
              (1 to MH) foreach { iter =>
                s = entity(i)._2(iter - 1)
                t = entity(i)._2(iter)
                val Cwt = cw(t)
                val Cws = cw(s)
                val Cs = c(s)
                val Ct = c(t)
                val π = math.min(1.0, (Cwt + β) / (Cws + β) * (Cs + βb) / (Ct + βb))
                if (uniform.random < π) {
                  z = t
                }

              }
              z
            }
              word -> Entity2(topic_indicator.toArray, entity.mcmc)
          }
        }
        cwk = cal_cwk2(byColumn)
        c = cal_ck2(cwk)
        val aliasMap = cwk.map{case(word,topic)=>
          val AliasTable = new aliasTable(topic)
          AliasTable.build()
            word->AliasTable
        }

        val updateMcmc = byColumn.map{doc =>
          doc.map{case (word, entity) =>
            val alias = aliasMap(word)
            (word, Entity2(entity.z, Array.fill(MH,entity.indicies)(alias.apply())))
          }
        }

        val updateMcmc2 = updateMcmc.map { doc =>
          val cd = cal_cd(doc.map(_._2))
          val update_topicIndicator = doc.map {case (word, entity) =>
            val topic_indicator = (0 to entity.indicies) map { i =>
              var s, t = 0
              var z = 0
              (1 to MH) foreach {iter =>
                s = entity(i)._2(iter - 1)
                t = entity(i)._2(iter)
                val Cdt = cd(t)
                val Cds = cd(s)
                val Cs = c(s)
                val Ct = c(t)
                val π = math.min(1.0, (Cdt + αC(t)) / (Cds + αC(s)) * (Cs + αS) / (Ct + αS))
                if (uniform.random < π) {
                  z = t
                }
              }
              z
            }
            word -> Entity2(topic_indicator.toArray, entity.mcmc)
          }
          val indicatorPool = doc.flatMap(_._2.z)
          val indicatorPoolLength = indicatorPool.length
          val update_Mcmc = update_topicIndicator.map{case (word, entity)=>
              val length = entity.indicies
              word -> Entity2(entity.z,Array.fill(MH,length)(update_zd(indicatorPool,indicatorPoolLength)))
          }
          update_Mcmc
        }
        block = updateMcmc
    }

  }
}
