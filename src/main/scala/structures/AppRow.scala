package structures

import scala.reflect.ClassTag

/**
  * Created by chris on 12/7/17.
  */
class AppRow[T:ClassTag](implicit ev: String => T) {
  var rowId:Int = _
  var values:Array[T] = _
  var columns:Array[Int] = _
  var len:Int = _
  var flag:Boolean = _

  def this(str:String)(implicit ev: String => T) {
    this()
    parse(str)(ev)
  }

  def parse(str:String)(ev:String => T) :Unit = {
    flag = if(str.split(" ")(0) == "1") true else false
    rowId = Integer.parseInt(str.split(" ")(1))
    val features = str.split(" ").slice(2, str.split(" ").length)
    len = features.length
    values = Array.ofDim[T](len)
    columns = Array.ofDim[Int](len)
    (0 until len) foreach{i =>
      val ind = Integer.parseInt(features(i).split(":")(0))
      val value = ev.apply(features(i).split(":")(1))
      columns(i) = ind
      values(i) = value
    }
  }
}
