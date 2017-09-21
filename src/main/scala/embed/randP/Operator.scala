package embed.randP


import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.math.vector.SparseFloatVector
import embed.randP.psf.PartCSRResult
import org.apache.commons.logging.LogFactory

/**
  * Created by chris on 9/19/17.
  */
object Operator {
  private val LOG = LogFactory.getLog(classOf[Operator])
}

class Operator(data:Matrix,model: RModel) {
  val wk = new Array[Float](model.F)

  def initialize(pkey: PartitionKey): Unit = {
    val rs: Int = pkey.getStartRow
    val re: Int = pkey.getEndRow
    val rand: Sampling = new Sampling(model.R, model.S)
    (rs until re) foreach { r =>
      val update = new SparseFloatVector(model.F)
      (0 until model.F) foreach { k =>
        val v = rand.apply()

        if(v != 0f) {
          update.plusBy(k, v)
        }
      }
      model.wtMat.increment(r, update)
    }
  }

  def multiply(bkeys: (Int, Int), csr:PartCSRResult, pkey:PartitionKey, partialResult:Array[Array[Float]]):Unit = {
    val (bs,be) = bkeys
    val ps = pkey.getStartRow
    val pe = pkey.getEndRow
    (ps until pe) foreach{ pr =>
      csr.read(wk)
      (bs until be) foreach{br =>
        var prSum = 0f
        (data.accRows(br) until data.accRows(br + 1)) foreach {i =>
          val id = data.indexes(i)
          val value = data.values(i)
          prSum += wk(id) * value
        }
        partialResult(br - bs)(pr) = prSum
      }
    }
  }
}
