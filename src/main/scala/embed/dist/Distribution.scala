package embed.dist

/**
  * Created by takun on 01/08/2017.
  */
trait Distribution {
  def apply(x:Double) : Double
  def random : Double
}
