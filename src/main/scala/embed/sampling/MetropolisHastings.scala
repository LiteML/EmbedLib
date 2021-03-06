package embed.sampling

import embed.dist.Distribution

/**
  * Created by takun on 01/08/2017.
  */
class MetropolisHastings(p:Distribution, q:Distribution, epoch:Int) extends DoubleSampling{
  def apply() = {
    var xt = 0.0
    var t = 0
    while( t < epoch ) {
      val x = q.random
      val u = r.nextDouble
      val a = math.min( (p(x) / p(xt)) * (q(xt) / q(x)) , 1.0)
      if( u < a ) {
        xt = x
        t += 1
      }
    }
    xt
  }
}
