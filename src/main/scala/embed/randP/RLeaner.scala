package embed.randP
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}

import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import embed.randP.psf.{GetPartFunc, PartCSRResult}
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/19/17.
  */
class RLeaner(ctx:TaskContext, model:RModel, data:Matrix) extends MLLearner(ctx){
  val pkeys = PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.wtMat.getMatrixId())

  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???

  val queue = new LinkedBlockingQueue[Operations]()
  val executor = Executors.newFixedThreadPool(model.threadNum)

  val batchSize = 1000000
  val bkeys = (0 until data.numOfRows by batchSize) map {i =>
    (i, Math.min(data.numOfRows, i + batchSize))
  }

  def scheduleInit(): Unit = {
    class Task(operations: Operations, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        operations.initialize(pkey)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Operations(data, model))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }
    // update for wt
    model.wtMat.clock().get()
    // update for nk
  }

  def scheduleMultiply():Unit = {
    class Task(operations: Operations,pkey:PartitionKey,csr:PartCSRResult,dkey:(Int,Int),partResult:Array[ArrayBuffer[(Int,Float)]]) extends Thread {
      override def run():Unit = {
        operations.multiply(dkey,csr,pkey,partResult)
        queue.add(operations)
        }
      }
    val client = PSAgentContext.get().getMatrixTransportClient
    val func = new GetPartFunc(null)
    for (i <- 0 until model.threadNum) queue.add(new Operations(data, model))
    for(bkey <- bkeys) {
      val (bs, be) = bkey
      val iter = pkeys.iterator()
      val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
      val partResult = Array.fill(10)(ArrayBuffer[(Int, Float)]())
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
            val operation = queue.take()
            future.get() match {
              case csr: PartCSRResult => executor.execute(new Task(operation, pkey, csr, bkey,partResult))
              case _ => throw new AngelException("should by PartCSRResult")
            }
            futures.remove(pkey)
          }
        }
      }


    }


  }
}
