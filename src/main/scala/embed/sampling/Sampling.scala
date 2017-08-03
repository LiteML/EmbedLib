package embed.sampling

import java.util.Random

/**
  * Created by takun on 01/08/2017.
  */
trait Sampling extends Serializable{
  private var seed = System.currentTimeMillis()
  val r = new Random(seed)
  def apply():Double
  def setSeed(seed:Long):this.type = {
    this.seed = seed
    this
  }
  def getSeed = seed
}
