package embed.sampling

import embed.dist.{GaussianDist, Uniform}

/**
  * Created by takun on 01/08/2017.
  */
object TestMH {

  def main(args: Array[String]): Unit = {
    val gaussian = new GaussianDist(0,1)
    val uniform = new Uniform(-0.5,0.5)
    val mh = new MetropolisHastings(uniform, gaussian, 100)
    1 to 100 foreach {
      i => println(mh.apply() + ",")
    }
  }
}
