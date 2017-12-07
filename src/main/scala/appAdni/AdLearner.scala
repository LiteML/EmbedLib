package appAdni

import java.io.BufferedOutputStream
import java.util
import java.util.{Collections, Random}
import java.util.concurrent.{ExecutorService, Executors, Future, LinkedBlockingQueue}

import adni.psf.{FloatPartCSRResult, GetFloatPartFunc, ListAggrResult}
import adni.utils.AtomicFloat
import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseFloatVector, DenseIntVector}
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import structures.CSRMatrix

import scala.collection.Set
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.Breaks._

/**
  * Created by chris on 11/3/17.
  */
class AdLearner(ctx:TaskContext, model:AdniModel,
                data:CSRMatrix[Float], rowId: Array[Int],
                seeds:Set[Int]) extends MLLearner(ctx){
  val LOG:Log = LogFactory.getLog(classOf[AdLearner])
  val pkeys:util.List[PartitionKey]= PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.mVec.getMatrixId())

  Collections.shuffle(pkeys, new Random(0))
  val numOfParts:Int = pkeys.length
  val numOfWorkers:Int = ctx.getTotalTaskNum
  val tmp:ArrayBuffer[Int] = ArrayBuffer[Int]()
  (0 until numOfParts by numOfWorkers) foreach {i =>
    val z = i + ctx.getTaskIndex
    if(z < numOfParts) {
      tmp.append(pkeys.get(z).getPartitionId)
    }
  }
  val locals:Array[Int] = tmp.toArray
  var trunc:Array[Float] =_
  val biject:Map[Int,Int] = rowId.zipWithIndex.toMap
  var userList:mutable.Buffer[Integer] = _
  var qualify:Boolean = false


  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???

  /**
    * Initialize the model
    */
  def initialize(): Unit = {
    val degVec = new DenseFloatVector(model.V)
    val degree = data.sum(axis = 0)

    degree.indices.foreach{ i =>
      degVec.plusBy(rowId(i), degree(i))
    }
    trunc = degree.map{f =>
      f * model.epslion * model.shrink
    }
    MMatrix(degree)
    model.mVec.increment(1, degVec)
    val sedVec = new DenseFloatVector(model.V)
    seeds.foreach{ i =>
      sedVec.plusBy(i,1f)
    }
    model.mVec.increment(0,sedVec)
    model.mVec.clock().get()
    ctx.incEpoch()
  }

  val queue:LinkedBlockingQueue[AdOperator] = new LinkedBlockingQueue[AdOperator]()
  val executor:ExecutorService = Executors.newFixedThreadPool(model.threadNum)


  /**
    * Matrix Multiplication
    */
  def scheduleMultiply(): Unit = {
    class Task(operator: AdOperator, pkey:PartitionKey, csr:FloatPartCSRResult, result :Array[AtomicFloat], original:Array[Float], biject:Map[Int,Int]) extends Thread {
      override def run():Unit = {
        operator.multiply(csr,result,original, biject)
        queue.add(operator)
      }
    }
    val original = Array.ofDim[Float](data.numOfRows)
    val result = Array.fill(data.numOfRows)(new AtomicFloat())
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
      if(result(i).get >= trunc(i)) {
        update.set(rowId(i),result(i).get - original(i))

      } else {
        update.set(rowId(i), -original(i))
      }
    }

    model.mVec.increment(0, update)
    model.mVec.clock().get()
  }

  /**
    * Check whether conditions are satisfied
    * @param epochNum
    */

  def ConditionsCheck(epochNum:Int): Unit = {
    val sVec = model.mVec.get(new psf.SSetFunc(model.mVec.getMatrixId(), locals)) match {
      case r : ListAggrResult => r.getResult
      case _ => throw new AngelException("should be ListAggrResult")
    }
      var j = model.k
      var userNum:Int = sVec.slice(0, j).count(p=> !seeds.contains(p.getKey))
      var degreeSum = sVec.slice(0, j).map{f =>
        f.getValue.getKey.toFloat
      }.sum
      breakable {
        while (j < sVec.size()) {
          degreeSum += sVec(j).getValue.getKey
          val cent = if (!seeds.contains(sVec(j).getKey)) 1 else 0
          userNum += cent
          val condition1 = userNum >= (model.k / numOfParts)
          val condition2 = (degreeSum >= math.pow(2, model.b) / numOfParts ) && (degreeSum < model.vol * 5.0 / 6 / numOfParts)
          val condition3 = sVec(j).getValue.getValue >= (1f / model.c4) * (model.l + 2) * math.pow(2, model.b)
          qualify = condition1 && condition2 && condition3
          j += 1
          if(qualify){
            break
          }
        }
      }

    if(qualify) {
      userList = sVec.slice(0, j + 1).flatMap{f=>
        if(!seeds.contains(f.getKey))
          Some(f.getKey)
        else
          None
      }
    }

    if(epochNum == model.epoch && userList == null) {
      userList = sVec.slice(0, model.k + 1).flatMap{f=>
        if(!seeds.contains(f.getKey))
          Some(f.getKey)
        else
          None
      }
    }
  }

  /**
    * Get the M matrix for Multiplication
    * @param degree
    */

  def MMatrix(degree:Array[Float]):Unit = {
    (0 until data.numOfRows) foreach{i=>
      (data.offSet(i) until data.offSet(i + 1)) foreach{z =>
        if(z == i) {
          data.values(z) /= (2 * degree(i))
          data.values(z) += (1f/2)
        } else {
          data.values(z) /= (2 * degree(i))
        }
      }
    }
  }

  /**
    * Model Training
    */

  def train() :Unit = {
    breakable{
      (1 to model.epoch) foreach{epoch =>
      scheduleMultiply()
      if(epoch % model.feq == 0) {
        if(locals.nonEmpty) {
          ConditionsCheck(epoch)
          val indiLast : DenseIntVector = model.indicator.getRow(0)
          val last:Int = indiLast.get(0)
          if(qualify){
            val indi = new DenseIntVector(1)
            indi.plusBy(0, 1)
            model.indicator.increment(0,indi)
            model.indicator.clock().get()
          }
          val indiNow: DenseIntVector = model.indicator.getRow(0)
          val now:Int = indiNow.get(0)
          if(now - last == math.min(numOfWorkers, numOfParts))
            break()
        }
        }
        ctx.incEpoch()
      }
    }
  }

  /**
    * Result Saving
    */
  def saveResult(): Unit = {
    LOG.info(s"save model")
    val dir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir
    val part = ctx.getTaskIndex
    val dest = new Path(base,part.toString)
    val fs = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp, 1.toShort))
    val sb = new mutable.StringBuilder()
    if(userList == null || userList.isEmpty){
      sb.append(s"${ctx.getTaskIndex} worker has no propagation!\n")
    } else {
      sb.append(userList.mkString("\n"))
    }
    out.write(sb.toString().getBytes("UTF-8"))
    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }
}
