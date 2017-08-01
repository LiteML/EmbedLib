package embed.sampling

import scala.util.Random

/**
  * Created by takun on 01/08/2017.
  */
trait Sampling {
  private var seed = System.currentTimeMillis()
  val r = new Random(seed)
  def apply():Double
  def setSeed(seed:Long):this.type = {
    this.seed = seed
    this
  }
  def getSeed = seed
}
