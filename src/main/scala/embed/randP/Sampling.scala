package embed.randP

import java.util.Random

/**
  * Created by chris on 9/19/17.
  */
class Sampling(numComponents:Int, s:Int) {
  val r:Random = new Random(System.currentTimeMillis())
  def apply(): Float = {
    val result = if( r.nextFloat() < 1f / 2*s) {
      - math.sqrt(s / numComponents.toDouble)
    } else if (r.nextFloat() > 1 - 1f/2*s){
      math.sqrt(s / numComponents.toDouble)
    } else {
      0d
    }
    result.toFloat
  }
}
