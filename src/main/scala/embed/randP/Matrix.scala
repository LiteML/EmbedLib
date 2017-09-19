package embed.randP

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/18/17.
  */
class Row {
  var rId:Int = _
  var len: Int = _
  var ids: Array[Short] = _
  var value:Array[Short] = _
  def this(row:String) {
    this()
    val parts = row.split("\t")
    this.rId = Integer.parseInt(parts(0))
    this.len = parts(1).split(" ").length
    val tmp = parts(1).split(" ").map{entry =>
      (entry.split(":")(0).toShort,entry.split(":")(1).toShort)
    }
    this.ids = tmp.map(_._1)
    this.value = tmp.map(_._2)
  }

}

class Matrix(val numOfRows:Int) {
  var accRows :Array[Int] = _
  var values : Array[Short] = _
  var indexes:Array[Short] = _
  var lengths:Int =_
  var rowIds:Array[Int] = _
  def build(rows :ArrayBuffer[Row]): Unit = {
    var rowLens:Array[Short] = _
    accRows = Array.ofDim[Int](numOfRows + 1)
    rows.indices foreach{i =>
      val row = rows(i)
      lengths += row.len
      rowLens(i) = row.len.toShort
      rowIds(i) = row.rId
    }
    values = Array.ofDim[Short](lengths)
    indexes = Array.ofDim[Short](lengths)
    accRows(0) = 0
    (0 until numOfRows) foreach {i=>
      accRows(i+1) = accRows(i) + rowLens(i)
    }
    var start = 0
    rows.indices.foreach{i=>
      val row = rows(i)
      row.ids.indices foreach{ i =>
        indexes(start) = row.ids(i)
        values(start) = row.value(i)
        start += 1
      }
    }
  }
}
