package embed.sampling

/**
  * Created by takun on 02/08/2017.
  */
class Dice(k:Int) extends IntSampling{

  def apply() = r.nextInt(k)

}
