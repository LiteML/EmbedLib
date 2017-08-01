package embed.dist

import embed.sampling.BoxMuller

/**
  * Created by takun on 01/08/2017.
  */
class GaussianDist extends Pdf{

  val r = new BoxMuller

  def random: Double = r()
  def apply(x:Double) = ???

}
