package embed.dist

import embed.sampling.Sampling

/**
  * Created by takun on 02/08/2017.
  */
class Dice(k:Int) extends Sampling{

  def apply() = r.nextInt(k)

}
