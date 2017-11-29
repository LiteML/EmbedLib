package embed.randP
import java.io.BufferedOutputStream
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}

import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import psf.{FloatPartCSRResult, GetFloatPartFunc}
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path

/**
  * Created by chris on 9/19/17.
  */
class RLearner(ctx:TaskContext, model:RModel, data:Matrix) extends MLLearner(ctx){
  val LOG:Log = LogFactory.getLog(classOf[RLearner])

  val pkeys = PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.wtMat.getMatrixId())

  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???

  val queue = new LinkedBlockingQueue[Operator]()
  val executor = Executors.newFixedThreadPool(model.threadNum)

  val batchSize = model.batchSize
  val bkeys = (0 until data.numOfRows by batchSize) map {i =>
    (i, Math.min(data.numOfRows, i + batchSize))
  }

  def scheduleInit(): Unit = {
    class Task(operator: Operator, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        operator.initialize(pkey)
        queue.add(operator)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Operator(data, model))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val operator = queue.take()
      executor.execute(new Task(operator, iter.next()))
    }

    for (i <- 0 until model.threadNum) queue.take()

    // update for wt
    model.wtMat.clock().get()
  }

  def scheduleMultiply():Unit = {
    class Task(operator: Operator, pkey:PartitionKey, csr:FloatPartCSRResult, dkey:(Int,Int), partResult:Array[Array[Float]]) extends Thread {
      override def run():Unit = {
        operator.multiply(dkey,csr,pkey,partResult)
        queue.add(operator)
        }
      }
    val client = PSAgentContext.get().getMatrixTransportClient
    val func = new GetFloatPartFunc(null)
    bkeys.indices foreach { i =>
      val bkey = bkeys(i)
      val (bs, be) = bkey
      val iter = pkeys.iterator()
      val len = be - bs

      for (i <- 0 until model.threadNum) queue.add(new Operator(data, model))

      val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()

      val partResult = Array.ofDim[Float](len, model.R)

      while (iter.hasNext) {
        val pkey = iter.next()
        val param = new PartitionGetParam(model.wtMat.getMatrixId, pkey)
        val future = client.get(func, param)
        futures.put(pkey, future)
      }

      while (futures.nonEmpty) {
        val keys = futures.keySet.iterator
        while (keys.hasNext) {
          val pkey = keys.next()
          val future = futures(pkey)
          if (future.isDone) {
            val operator = queue.take()
            future.get() match {
              case csr: FloatPartCSRResult => executor.execute(new Task(operator, pkey, csr, bkey, partResult))
              case _ => throw new AngelException("should by FloatPartCSRResult")
            }
            futures.remove(pkey)
          }
        }
      }
      for (i <- 0 until model.threadNum) queue.take()
      if(model.saveMat) savePartResult(partResult,i,bkey)
    }
  }

  def savePartResult(result:Array[Array[Float]], batch:Int,block: (Int, Int)): Unit = {
    LOG.info(s"save $batch")
    val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + s"batch_$batch"
    val part = ctx.getTaskIndex
    val dest = new Path(base,part.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp, 1.toShort))
    val (bs, be) = block
    (bs until be) foreach { b =>
      val sb = new mutable.StringBuilder()
      sb.append(data.rowIds(b))
      val row = result(b - bs)
      row.zipWithIndex.filter(f => f._1 != 0f).foreach{case(v, k) =>
          sb.append(s" $k:$v")
      }
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }
    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }
}
