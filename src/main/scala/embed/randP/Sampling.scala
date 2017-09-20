package embed.randP

import java.util.Random

/**
  * Created by chris on 9/19/17.
  */
class Sampling(numComponents:Int, s:Double) {
  val r:Random = new Random(System.currentTimeMillis())
  def apply(): Float = {
    val result = if( r.nextDouble() < 1 / (2*s)){
      - math.sqrt(s / numComponents)
    } else if (r.nextDouble() > 1 - 1/ (2*s)){
      math.sqrt(s / numComponents)
    } else {
      0d
    }
    result.toFloat
  }
}
