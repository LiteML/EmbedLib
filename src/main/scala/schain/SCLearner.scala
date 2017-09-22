package schain

import java.io.BufferedOutputStream
import java.util.Collections
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}
import scala.util.Random
import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import embed.randP.psf.{GetPartFunc, PartCSRResult}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/**
  * Created by chris on 9/21/17.
  */
class SCLearner(ctx:TaskContext, data:SMatrix, model:SCModel) extends MLLearner(ctx) {

  val LOG:Log = LogFactory.getLog(classOf[SCLearner])

  val pkeyR = PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.wtMat.getMatrixId())

  Collections.shuffle(pkeyR)

  val pkeyM1 = PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.mat1.getMatrixId())

  Collections.shuffle(pkeyM1)

  val pkeyM2 = PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.mat2.getMatrixId())

  Collections.shuffle(pkeyM2)


  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???

  val queue = new LinkedBlockingQueue[SCOperator]()
  val executor = Executors.newFixedThreadPool(model.threadNum)

  val batchSize = model.batchSize
  var bkeys = (0 until data.numOfRows by batchSize) map {i =>
  (i, Math.min(data.numOfRows, i + batchSize))
  }


  def scheduleInit(): Unit = {
    class Task(operator: SCOperator, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        operator.initialize(pkey)
        queue.add(operator)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new SCOperator(data, model))

    val iter = pkeyR.iterator()
    while (iter.hasNext) {
      val operator = queue.take()
      executor.execute(new Task(operator, iter.next()))
    }

    for (i <- 0 until model.threadNum) queue.take()

    // update for wt
   model.wtMat.clock().get()
  }

  def scheduleMultiply():Unit = {
    class MultiTask(operator: SCOperator, pkey: PartitionKey, csr: PartCSRResult, dkey: (Int, Int), partResult: Array[Array[Float]], label:Int) extends Thread {
      override def run(): Unit = {
        operator.multiply(dkey, csr, pkey, partResult, label)
        queue.add(operator)
      }
    }

    class ProjTask(operator: SCOperator, batch : Array[Array[Float]], csr:PartCSRResult, pkey:PartitionKey, result:Array[Array[Float]]) extends Thread {
      override def run(): Unit = {
        operator.proj(batch,csr,pkey, result)
        queue.add(operator)
      }
    }

  val client = PSAgentContext.get().getMatrixTransportClient
  val func = new GetPartFunc(null)


  bkeys.indices foreach { i =>
    val bkey = bkeys(i)
    val (bs, be) = bkey
    val len = be - bs
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
    var batch = Array.ofDim[Float](len, model.N)
    for (i <- 0 until model.threadNum) queue.add(new SCOperator(data, model))
    val iter1 = pkeyM1.iterator()
    while (iter1.hasNext) {
      val pkey = iter1.next()
      val param = new PartitionGetParam(model.mat1.getMatrixId, pkey)
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
            case csr: PartCSRResult => executor.execute(new MultiTask(operator, pkey, csr, bkey, batch, 1))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    val iter2 = pkeyM2.iterator()
    while (iter2.hasNext) {
      val pkey = iter2.next()
      val param = new PartitionGetParam(model.mat2.getMatrixId, pkey)
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
            case csr: PartCSRResult => executor.execute(new MultiTask(operator, pkey, csr, bkey, batch, 2))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    val result: Array[Array[Float]] = Array.ofDim[Float](len, model.R)

    val iter3 = pkeyR.iterator()
    while (iter3.hasNext) {
      val pkey = iter3.next()
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
            case csr: PartCSRResult => executor.execute(new ProjTask(operator, batch, csr, pkey, result))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    batch = null
    (0 until model.threadNum) foreach{ _ => queue.take() }
    if (model.saveMat) savePartResult(result, i, bkey)
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
