package schain

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/20/17.
  */

class SRow {
  var rId:Int = _
  var len1: Int = _
  var len2: Int = _
  var id1: Array[Short] = _
  var id2: Array[Short] = _
  var value1:Array[Float] = _
  var value2:Array[Float] = _
  def this(dim1:Int, row:String) {
    this()
    val parts = row.split("\t")
    this.rId = Integer.parseInt(parts(0))
    val tmp = parts(1).split(" ").map{entry =>
      (entry.split(":")(0).toShort,entry.split(":")(1).toFloat)
    }
    val tmp1 = tmp.filter(f => f._1 < dim1)
    val tmp2 = tmp.filter(f => f._1 >= dim1)
    this.len1 = tmp1.length
    this.len2 = tmp2.length
    this.id1 = tmp1.map(_._1)
    this.value1 = tmp1.map(_._2)
    this.id2 = tmp2.map(f => (f._1 - dim1).toShort)
    this.value2 = tmp2.map(_._2)
  }

}

class SMatrix(val numOfRows:Int) {
  var accRow1 :Array[Int] = _
  var accRow2 :Array[Int] = _
  var value1 : Array[Float] = _
  var value2 : Array[Float] = _
  var index1:Array[Short] = _
  var index2:Array[Short] = _
  var length1:Int =_
  var length2:Int = _
  var rowIds:Array[Int] = _
  def build(rows:ArrayBuffer[SRow]):Unit = {
    var rowLen1 = Array.ofDim[Short](numOfRows)
    var rowLen2 = Array.ofDim[Short](numOfRows)
    rowIds = Array.ofDim[Int](numOfRows)
    accRow1 = Array.ofDim[Int](numOfRows + 1)
    accRow2 = Array.ofDim[Int](numOfRows + 1)
    rows.indices.foreach{i =>
      val row = rows(i)
      length1 += row.len1
      length2 += row.len2
      rowLen1(i) = row.len1.toShort
      rowLen2(i) = row.len2.toShort
      rowIds(i) = row.rId
    }
    index1 = Array.ofDim[Short](length1)
    value1 = Array.ofDim[Float](length1)
    index2 = Array.ofDim[Short](length2)
    value2 = Array.ofDim[Float](length2)

    accRow1(0) = 0
    (0 until numOfRows) foreach {i=>
      accRow1(i+1) = accRow1(i) + rowLen1(i)
    }

    rowLen1 = null

    accRow2(0) = 0
    (0 until numOfRows) foreach {i=>
      accRow2(i+1) = accRow2(i) + rowLen2(i)
    }

    rowLen2 = null

    var start1 = 0
    var start2 = 0
    rows.indices.foreach{i=>
      val row = rows(i)
      row.id1.indices foreach{ i =>
        index1(start1) = row.id1(i)
        value1(start1) = row.value1(i)
        start1 += 1
      }

      row.id2.indices foreach{ i =>
        index2(start2) = row.id2(i)
        value2(start2) = row.value2(i)
        start2 += 1
      }
    }

  }

}
