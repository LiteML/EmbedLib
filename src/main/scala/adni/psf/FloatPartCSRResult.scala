package adni.psf

import java.nio.FloatBuffer

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult
import com.tencent.angel.ps.impl.matrix.{ServerDenseFloatRow, ServerRow}
import io.netty.buffer.ByteBuf
import org.apache.commons.logging.LogFactory

import scala.collection.mutable

/**
  * Created by chris on 11/6/17.
  */
object FloatPartCSRResult {
  private val LOG = LogFactory.getLog(classOf[FloatPartCSRResult])
}

class FloatPartCSRResult() extends PartitionGetResult {
  private var row: ServerRow = _
  private var buf: ByteBuf = _
  private var len: Int = 0
  private var readerIdx: Int = 0

  def this(row: ServerRow) {
    this()
    this.row = row
  }

  override def serialize(buf: ByteBuf): Unit = { // Write #rows
    buf.writeInt(1)
    // Write each row
    serialize(buf, row.asInstanceOf[ServerDenseFloatRow])
  }

  def serialize(buf: ByteBuf, row: ServerDenseFloatRow): Unit = {
    try {
      row.getLock.readLock.lock()
      val m:FloatBuffer = row.getData
      val partLens:Int = (row.getEndCol - row.getStartCol).toInt
      var cnt = 0
      (0 until partLens) foreach{i =>
        if(m.get(i) > 0.0f)
          cnt += 1
      }
      buf.writeInt(cnt)
      (0 until partLens) foreach{i =>
          if (m.get(i) > 0.0f){
            buf.writeInt(row.getStartCol.toInt + i)
            buf.writeFloat(m.get(i))
          }
      }
    }
    finally {
      row.getLock.readLock.unlock()
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    this.len = buf.readInt
    this.buf = buf.duplicate
    this.buf.retain
    //    LOG.info(buf.refCnt());
    this.readerIdx = 0
  }

  override def bufferLen: Int = {
     48
  }

  def read(row: mutable.Map[Int, Float]): Boolean = {
    if (readerIdx == this.len) {
      return false
    }
    readerIdx += 1
    // sparse
    row.clear()
    val len: Int = buf.readInt
    var i: Int = 0
    while (i < len) {
      val k: Int = buf.readInt
      val v: Float = buf.readFloat
      row.put(k, v)
      i += 1
    }
    if (readerIdx == this.len) {
      buf.release
      //      LOG.info(buf.refCnt());
    }
    true
  }
}
