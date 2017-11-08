package adni

import java.util
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}

import adni.psf._
import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseFloatVector
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import structures.CSRMatrix

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
/**
  * Created by chris on 11/3/17.
  */
class AdLearner(ctx:TaskContext, model:AdniModel,
                data:CSRMatrix[Float], rowId: Array[Int],
                seeds:Array[Int]) extends MLLearner(ctx){
  val LOG:Log = LogFactory.getLog(classOf[AdLearner])
  val pkeys: util.List[PartitionKey]= PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.mVec.getMatrixId())
  var trunc:Array[Float] =_
  val biject:Map[Int,Int] = rowId.zipWithIndex.toMap
  var userList:Array[Int] = _
  var qualify = false


  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???

  def initialize(): Unit = {
    val degVec = new DenseFloatVector(model.V)
    val degree = data.sum(axis = 0)
    degree.indices.foreach{ i =>
      degVec.plusBy(rowId(i), degree(i))
    }
    trunc = degree.map{f =>
      f * model.epslion
    }
    MMatrix(degree)
    model.mVec.increment(1, degVec)
    val sedVec = new DenseFloatVector(model.V)
    seeds.foreach{ i =>
      sedVec.plusBy(i,1f)
    }
    model.mVec.increment(0,sedVec)
    model.mVec.clock().get()
    val ll = 0
    ctx.incEpoch()
  }

  val queue = new LinkedBlockingQueue[AdOperator]()
  val executor = Executors.newFixedThreadPool(model.threadNum)

  def scheduleMultiply(): Unit = {
    class Task(operator: AdOperator, pkey:PartitionKey, csr:FloatPartCSRResult, result:Array[Float], original:Array[Float], biject:Map[Int,Int]) extends Thread {
      override def run():Unit = {
        operator.multiply(csr,result,original, biject)
        queue.add(operator)
      }
    }
    val original:Array[Float] = Array.ofDim[Float](data.numOfRows)
    val result = Array.ofDim[Float](data.numOfRows)
    val client = PSAgentContext.get().getMatrixTransportClient
    val func = new GetFloatPartFunc(null)
    for (i <- 0 until model.threadNum) queue.add(new AdOperator(data, model))

    val iter = pkeys.iterator()
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
    while (iter.hasNext) {
      val pkey = iter.next()
      val param = new PartitionGetParam(model.mVec.getMatrixId, pkey)
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
            case csr: FloatPartCSRResult => executor.execute(new Task(operator, pkey, csr, result, original, biject))
            case _ => throw new AngelException("should by FloatPartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    for (i <- 0 until model.threadNum) queue.take()

    val update:DenseFloatVector = new DenseFloatVector(model.V)

    (0 until data.numOfRows) foreach { i =>
      if(original(i) >= 0 && result(i) >= trunc(i)) {
        update.set(rowId(i),result(i) - original(i))

      } else if(original(i) > 0 && result(i) < trunc(i)){
        update.set(rowId(i), -original(i))
      }
    }

    model.mVec.increment(0, update)
    model.mVec.clock().get()
  }

  def ConditionCheck():Unit = {

    val sVec = model.mVec.get(new SSetFunc(model.mVec.getMatrixId())) match {
      case r : ListAggrResult => r.getResult
      case _ => throw new AngelException("should be ListAggrResult")
    }

    val totalUsers = sVec.map { f =>
      if (f.getKey < model.u) 1 else 0
    }.sum
    if(totalUsers < model.k) {
      qualify = false
    } else {
      var j = model.k
      var userNum = sVec.slice(0, j).map{f =>
        if(f.getKey < model.u) 1 else 0
      }.sum
      var degreeSum = sVec.slice(0, j).map{f =>
        f.getValue.getKey
      }.sum
      breakable {
        while (j < sVec.size()) {
          degreeSum += sVec(j).getValue.getKey
          val cent = if (sVec(j).getKey < model.u) 1f else 0f
          userNum += cent
          val condition1 = userNum > model.k
          val condition2 = (degreeSum >= (2 << model.b)) && (degreeSum < model.vol * 5.0 / 6)
          val condition3 = sVec(j).getValue.getValue >= (1f / model.c4) * (model.l + 2) * (2 << model.b)
          qualify = condition1 && condition2 && condition3
          if(qualify) {
            userList = sVec.slice(0, j + 1).map{f=>
              f.getKey.toInt
            }.toArray
            break
          } else {
            j += 1
          }
        }
      }
      }
    }

  def MMatrix(degree:Array[Float]):Unit = {
    (0 until data.numOfRows) foreach{ i=>
      (data.offSet(i) until data.offSet(i + 1)) foreach{z =>
        if(z == i) {
          data.values(z) /= (2 * degree(i))
          data.values(z) += (1/2)
        } else {
          data.values(z) /= (2 * degree(i))
        }
      }
    }
  }

  def train() :Unit = {
    breakable{
      (1 to model.tlast) foreach{epoch =>
      scheduleMultiply()
      if(epoch % 10 == 0) {
        if(ctx.getTaskIndex == 0) {
          ConditionCheck()
          if(qualify){
            break
          }
        }
      }
        ctx.incEpoch()
      }
    }
  }
}