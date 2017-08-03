package embed.sampling

/**
  * Created by takun on 01/08/2017.
  */
object TestBoxMuller {

  def main(args: Array[String]): Unit = {
    val r = new BoxMuller()
    1 to 1000 foreach{
      i => println(r()+",")
    }
  }
}
