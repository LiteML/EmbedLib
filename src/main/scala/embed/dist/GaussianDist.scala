package embed.dist

import embed.sampling.BoxMuller

/**
  * Created by takun on 01/08/2017.
  */
class GaussianDist(mean:Double, variance:Double) extends Distribution{

  val r = new BoxMuller

  def random: Double = r()
  def apply(x:Double) = ???

}
