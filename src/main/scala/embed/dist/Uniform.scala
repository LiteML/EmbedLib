package embed.dist

import scala.util.Random

/**
  * Created by takun on 01/08/2017.
  */
class Uniform(x1:Double,x2:Double) extends Distribution{
  private[this] val r = new Random()
  private[this] val range = x2 - x1
  def random = x1 + r.nextDouble() * range
  def apply(x:Double) = 1.0 / range
}