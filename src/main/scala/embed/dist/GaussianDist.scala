package embed.dist

import embed.sampling.BoxMuller

/**
  * Created by takun on 01/08/2017.
  */
class GaussianDist(mean:Double, variance:Double) extends Distribution{

  val std = math.sqrt(variance)
  val r = new BoxMuller

  def random: Double = r()
  def apply(x:Double) = math.exp(-math.pow(x - mean, 2.0) / (2 * variance)) / math.sqrt(2 * Math.PI * std)

}
