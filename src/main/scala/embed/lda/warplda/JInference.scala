package embed.lda.warplda

import java.util
import java.util.Collections
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}

import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf.LOG_LIKELIHOOD
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.metric.log.ObjMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import embed.lda.LDAModel
import embed.lda.warplda.get.{GetPartFunc, LikelihoodFunc, PartCSRResult}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.commons.math.special.Gamma

/**
  * Created by chris on 9/8/17.
  */
class JTrainer(ctx:TaskContext, model:LDAModel,
               data:JWTokens) extends MLLearner(ctx){
  val LOG:Log = LogFactory.getLog(classOf[JTrainer])
  val pkeys: util.List[PartitionKey]= PSAgentContext.get().getMatrixPartitionRouter.
    getPartitionKeyList(model.wtMat.getMatrixId())
  val dKeys:Int = data.n_docs


  Collections.shuffle(pkeys)

  // Hyper parameters
  val alpha:Float = model.alpha
  val beta:Float  = model.beta
  val lgammaBeta:Double = Gamma.logGamma(beta)
  val lgammaAlpha:Double = Gamma.logGamma(alpha)
  val lgammaAlphaSum:Double = Gamma.logGamma(alpha * model.K)
  var ll:Double = 0
  var nnz:Int = 0


  val nk = new Array[Int](model.K)

  globalMetrics.addMetrics(LOG_LIKELIHOOD, new ObjMetric())

  /**
    * Train a ML Model
    *
    * @param train : input train data storage
    * @param vali  : validate data storage
    * @return : a learned model
    */
  override
  def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = ???
  def initialize(): Unit = {
    scheduleInit()
    likelihood
    LOG.info(s"ll=$ll")
    globalMetrics.metrics(LOG_LIKELIHOOD, ll)
    ctx.incIteration()
  }

  def reset(epoch: Int):Unit = {
    LOG.info(s"start reset")
    model.tMat.getRow(0)
    if (ctx.getTaskIndex == 0) {
      model.tMat.zero()
      model.wtMat.zero()
    }
    model.tMat.clock(false)
    model.tMat.getRow(0)
    LOG.info(s"finish reset")
  }



  def inference(n_iters: Int): Unit = {
    for (i <- 1 to n_iters) {
      ctx.incIteration()
    }
  }




  def computeWordLLH: Double = {
    model.wtMat.get(new LikelihoodFunc(model.wtMat.getMatrixId(), beta)) match {
      case r : ScalarAggrResult => r.getResult
      case _ => throw new AngelException("should be ScalarAggrResult")
    }
  }

  def computeWordLLHSummary: Double = {
    var ll = model.K * Gamma.logGamma(beta * model.V)
    for (k <- 0 until model.K)
      ll -= Gamma.logGamma(nk(k) + beta * model.V)
    ll
  }

  def likelihood: Unit = {
    fetchNk
    if (ctx.getTaskIndex == 0)
      ll += computeWordLLHSummary + computeWordLLH
    ll -= nnz * Gamma.logGamma(alpha)
    ll += data.n_docs * Gamma.logGamma(alpha * model.K)
  }

  def fetchNk: Unit = {
    val row = model.tMat.getRow(0)
    var sum = 0
    for (i <- 0 until model.K) {
      nk(i) = row.get(i)
      sum += nk(i)
    }

    LOG.info(s"nk_sum=$sum")
  }

  val queue = new LinkedBlockingQueue[JSampler]()
  val executor = Executors.newFixedThreadPool(model.threadNum)
  def scheduleInit(): Unit = {
    class Task(sampler: JSampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        sampler.initialize(pkey)
        queue.add(sampler)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new JSampler(data, model))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }

    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)
    // update for wt
    model.wtMat.clock().get()
    // update for nk
    model.tMat.clock().get()
  }


}
