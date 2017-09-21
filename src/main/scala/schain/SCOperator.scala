package schain

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.math.vector.SparseFloatVector
import embed.randP.Sampling
import embed.randP.psf.PartCSRResult

/**
  * Created by chris on 9/21/17.
  */
class SCOperator(data:SMatrix, model: SCModel) {
  val wk = new Array[Float](model.N)
  val wk1 = new Array[Float](model.DIM_1)
  val wk2 = new Array[Float](model.DIM_2)

  def initialize(pkey: PartitionKey): Unit = {
    val rs: Int = pkey.getStartRow
    val re: Int = pkey.getEndRow
    val rand: Sampling = new Sampling(model.R, model.S)
    (rs until re) foreach { r =>
      val update = new SparseFloatVector(model.N)
      (0 until model.N) foreach { k =>
        val v = rand.apply()
        if(v != 0f) {
          update.plusBy(k, v)
        }
      }
      model.wtMat.increment(r, update)
    }
  }

  def multiply(bkeys: (Int, Int), csr:PartCSRResult, pkey:PartitionKey, partialResult:Array[Array[Float]], label:Int):Unit = {
    val (bs,be) = bkeys
    val ps = pkey.getStartRow
    val pe = pkey.getEndRow
    (ps until pe) foreach{ pr =>
      label match {
        case 1 => csr.read(wk1)
        case 2 => csr.read(wk2)
      }
      (bs until be) foreach{br =>
        var prSum = 0f
        label match {
          case 1 =>
            (data.accRow1(br) until data.accRow1(br + 1)) foreach {i =>
              val id = data.index1(i)
              val value = data.value1(i)
              prSum += wk1(id) * value
            }
          case 2 =>
            (data.accRow2(br) until data.accRow2(br + 1)) foreach {i =>
              val id = data.index2(i)
              val value = data.value2(i)
              prSum += wk2(id) * value
            }
        }
        partialResult(br - bs)(pr) += prSum
      }
    }
  }

  def proj(batch : Array[Array[Float]], csr:PartCSRResult, pkey:PartitionKey, result:Array[Array[Float]]): Unit = {
    val ps = pkey.getStartRow
    val pe = pkey.getEndRow
    (ps until pe) foreach { pr =>
      csr.read(wk)
      batch.indices foreach{ i =>
        var prSum = 0f
        batch(i).indices.foreach{ j =>
          prSum += batch(i)(j) * wk(j)
        }
        result(i)(pr) += prSum
      }
    }
  }
}
