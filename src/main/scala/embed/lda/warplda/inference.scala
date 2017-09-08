package embed.lda.warplda

import java.io.BufferedOutputStream
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, Future, LinkedBlockingQueue}

import com.tencent.angel.PartitionKey
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf.LOG_LIKELIHOOD
import com.tencent.angel.ml.feature.LabeledData
import embed.lda.warplda.get.{GetPartFunc, LikelihoodFunc}
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.{PartitionGetParam, PartitionGetResult}
import com.tencent.angel.ml.metric.log.ObjMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.utils.HdfsUtil
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import embed.lda.LDAModel
import embed.lda.warplda.get.PartCSRResult
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.commons.math.special.Gamma
import org.apache.hadoop.fs.Path

import scala.collection.mutable

/**
  * Created by chris on 8/24/17.
  */
class Trainer(ctx:TaskContext, model:LDAModel,
              data:WTokens) extends MLLearner(ctx){
  val LOG:Log = LogFactory.getLog(classOf[Trainer])
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
    scheduleReset()
    LOG.info(s"finish reset")
  }

  def initForInference(): Unit = {
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        sampler.initForInference(pkey)
        queue.add(sampler)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model))

    val iter = pkeys.iterator()
    while (iter.hasNext) {
      val sampler = queue.take()
      executor.execute(new Task(sampler, iter.next()))
    }

    for (i <- 0 until model.threadNum) queue.take()

    model.tMat.clock(false).get()
    ctx.incIteration()
  }

  def inference(n_iters: Int): Unit = {
    for (i <- 1 to n_iters) {
      sampleForWordInference()
      sampleForDocInference()
      ctx.incIteration()
    }
  }


  def sampleForDocInference():Unit = {
    class Task(sampler: Sampler, pkey: Int) extends Thread {
      override def run(): Unit = {
        sampler.docSample(pkey)
        queue.add(sampler)
      }
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    (0 until dKeys).foreach{dkey =>
      val sampler = queue.take()
      executor.execute(new Task(sampler, dkey))
    }

    for (i <- 0 until model.threadNum) queue.take()
    model.tMat.clock(false).get()
  }

  def sampleForWordInference(): Unit = {
    class Task(sampler: Sampler, pkey: PartitionKey, csr: PartCSRResult) extends Thread {
      override def run(): Unit = {
        sampler.wordInference(pkey, csr)
        queue.add(sampler)
      }
    }

    val client = PSAgentContext.get().getMatrixTransportClient
    val iter = pkeys.iterator()
    val func = new GetPartFunc(null)
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
    while (iter.hasNext) {
      val pkey = iter.next()
      val param = new PartitionGetParam(model.wtMat.getMatrixId, pkey)
      val future = client.get(func, param)
      futures.put(pkey, future)
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    while (futures.nonEmpty) {
      val keys = futures.keySet.iterator
      while (keys.hasNext) {
        val pkey = keys.next()
        val future = futures(pkey)
        if (future.isDone) {
          val sampler = queue.take()
          future.get() match {
            case csr: PartCSRResult => executor.execute(new Task(sampler, pkey, csr))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    for (i <- 0 until model.threadNum) queue.take()

    model.tMat.clock(false).get()
  }



  def scheduleReset(): Unit = {
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        sampler.reset(pkey)
        queue.add(sampler)
      }
    }

    util.Arrays.fill(nk, 0)
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))
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

 /* def scheduleDocllh(n_docs: Int):Double = {
    val results = new LinkedBlockingQueue[Double]()
    class Task(index: AtomicInteger) extends Thread {
      private var ll = 0.0
      override def run(): Unit = {
        while (index.get() < n_docs) {
          val d = index.incrementAndGet()
          if (d < n_docs) {
            () foreach {j=>
              ll += Gamma.logGamma(alpha + dk(j))
              ll -= Gamma.logGamma(data.docLens(d) + alpha * model.K)
            }
          }
        }
        results.add(ll)
      }
    }

    val index = new AtomicInteger(0)
    var ll = 0.0; var nnz = 0
    for (i <- 0 until model.threadNum) executor.execute(new Task(index))
    for (i <- 0 until model.threadNum) ll += results.take()
    for (d <- 0 until n_docs) nnz += data.nnz(d)
    ll -= nnz * Gamma.logGamma(alpha)
    ll += data.n_docs * Gamma.logGamma(alpha * model.K)
    ll
  }*/


  def fetchNk: Unit = {
    val row = model.tMat.getRow(0)
    var sum = 0
    for (i <- 0 until model.K) {
      nk(i) = row.get(i)
      sum += nk(i)
    }

    LOG.info(s"nk_sum=$sum")
  }

  val queue = new LinkedBlockingQueue[Sampler]()
  val executor = Executors.newFixedThreadPool(model.threadNum)
  def scheduleInit(): Unit = {
    class Task(sampler: Sampler, pkey: PartitionKey) extends Thread {
      override def run(): Unit = {
        sampler.initialize(pkey)
        queue.add(sampler)
      }
    }

    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model))

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


  def scheduleWordSample(pkeys: util.List[PartitionKey]): Boolean = {

    class Task(sampler: Sampler, pkey: PartitionKey, csr: PartCSRResult) extends Thread {
      override def run(): Unit = {
        sampler.wordSample(pkey, csr)
        queue.add(sampler)
      }
    }

    val client = PSAgentContext.get().getMatrixTransportClient
    val iter = pkeys.iterator()
    val func = new GetPartFunc(null)
    val futures = new mutable.HashMap[PartitionKey, Future[PartitionGetResult]]()
    while (iter.hasNext) {
      val pkey = iter.next()
      val param = new PartitionGetParam(model.wtMat.getMatrixId, pkey)
      val future = client.get(func, param)
      futures.put(pkey, future)
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    while (futures.nonEmpty) {
      val keys = futures.keySet.iterator
      while (keys.hasNext) {
        val pkey = keys.next()
        val future = futures(pkey)
        if (future.isDone) {
          val sampler = queue.take()
          future.get() match {
            case csr: PartCSRResult => executor.execute(new Task(sampler, pkey, csr))
            case _ => throw new AngelException("should by PartCSRResult")
          }
          futures.remove(pkey)
        }
      }
    }

    var error = false
    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      error = sampler.error
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)
    // update for wt
    model.wtMat.clock().get()
    // update for nk
    model.tMat.clock().get()

    error
  }


  def scheduleDocSample(dKeys:Int): Boolean = {
    class Task(sampler: Sampler, pkey: Int) extends Thread {
      override def run(): Unit = {
        sampler.docSample(pkey)
        sampler.dk.foreach{
          case(_ , n) =>
            ll += Gamma.logGamma(alpha + n)
            ll -= Gamma.logGamma(data.docLens(pkey) + alpha * model.K)
        }
        nnz += sampler.dk.size
        queue.add(sampler)
      }
    }

    // copy nk to each sampler
    for (i <- 0 until model.threadNum) queue.add(new Sampler(data, model).set(nk))

    (0 until dKeys).foreach{dkey =>
      val sampler = queue.take()
       executor.execute(new Task(sampler, dkey))
    }

    var error = false
    // calculate the delta value of nk
    // the take means that all tasks have been finished
    val update = new DenseIntVector(model.K)
    for (i <- 0 until model.threadNum) {
      val sampler = queue.take()
      error = sampler.error
      for (i <- 0 until model.K)
        update.plusBy(i, sampler.nk(i) - nk(i))
    }

    model.tMat.increment(0, update)
    // update for nk
    model.tMat.clock().get()
    error
  }

  def train(n_iters: Int): Unit = {

    for (epoch <- 1 to n_iters) {
      // One epoch
      fetchNk
      this.ll = 0d
      this.nnz = 0
      var error = scheduleWordSample(pkeys)
      error = scheduleDocSample(dKeys)

      // calculate likelihood
      likelihood
      LOG.info(s"epoch=$epoch local likelihood=$ll")

      // submit to client
      globalMetrics.metrics(LOG_LIKELIHOOD, ll)
      ctx.incIteration()

      //      if (epoch % 10 == 0) reset(epoch)
    }
  }

  def saveWordTopic(model: LDAModel): Unit = {
    LOG.info("save word topic")
    val dir  = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + "word_topic"
    val taskId = ctx.getTaskIndex
    val dest = new Path(base, taskId.toString)

    val fs  = dest.getFileSystem(conf)
    val tmp = HdfsUtil.toTmpPath(dest)
    val out = new BufferedOutputStream(fs.create(tmp, 1.toShort))


    val num = model.V / ctx.getTotalTaskNum + 1
    val start = taskId * num
    val end   = Math.min(model.V, start + num)

    val index = new RowIndex()
    for (i <- start until end) index.addRowId(i)
    val rr = model.wtMat.getRows(index, 1000)

    for (row <- start until end) {
      val x = rr(row)
      val len = x.size()
      val sb = new StringBuilder
      sb.append(x.getRowId + ":")
      for (i <- 0 until len)
        sb.append(s" ${x.get(i)}")
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }

  def saveDocTopic(data: WTokens, model: LDAModel): Unit = {
    LOG.info("save doc topic ")
    val dir  = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    val base = dir + "/" + "doc_topic"
    val part = ctx.getTaskIndex

    val dest = new Path(base, part.toString)
    val fs   = dest.getFileSystem(conf)
    val tmp  = HdfsUtil.toTmpPath(dest)
    val out  = new BufferedOutputStream(fs.create(tmp, 1.toShort))

    for (d <- 0 until data.n_docs) {
      val sb = new StringBuilder
      val dk = Array.ofDim[Int](model.K)
      (data.accDoc(d) until data.accDoc(d + 1)) foreach{ i=>
        dk(data.topics(data.inverseMatrix(i)))  += 1
      }
      sb.append(data.docIds(d))
      val sparseDk = dk.zipWithIndex.filter(_._1 != 0)
      val len = sparseDk.length
      sparseDk.foreach{s =>
        sb.append(s" ${s._2}:${s._1}")
      }
      sb.append("\n")
      out.write(sb.toString().getBytes("UTF-8"))
    }

    out.flush()
    out.close()
    fs.rename(tmp, dest)
  }
}
