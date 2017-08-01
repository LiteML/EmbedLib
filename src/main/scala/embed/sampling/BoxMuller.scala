package embed.sampling

/**
  * Created by takun on 01/08/2017.
  */
class BoxMuller extends Sampling{
  def apply() = {
    val u1 = r.nextDouble
    val u2 = r.nextDouble
    val R: Double = Math.sqrt(-2 * Math.log(u2))
    val theta: Double = 2 * Math.PI * u1
    R * Math.sin(theta)
  }
}