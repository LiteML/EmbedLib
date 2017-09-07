package embed.lda.warplda

import java.util.Random

import com.tencent.angel.PartitionKey
import com.tencent.angel.exception.AngelException
import embed.lda.warplda.get.PartCSRResult
import com.tencent.angel.ml.math.vector.DenseIntVector
import embed.lda.LDAModel
import embed.sampling.AliasTable
import org.apache.commons.logging.LogFactory

import scala.collection.mutable
import scala.util.control.Breaks._
/**
  * Created by chris on 8/23/17.
  */
object Sampler {
  private val LOG = LogFactory.getLog(classOf[Sampler])
}

class Sampler(var data: WTokens, var model: LDAModel) {
  val K:Int = model.K
  val alpha:Float = model.alpha
  val beta:Float = model.beta
  val dalpha:Float = data.n_docs * alpha
  val vbeta:Float = data.n_words * beta
  var nk = new Array[Int](K)
  var wk = new Array[Int](K)
  var dk: mutable.Map[Int, Int] = mutable.Map[Int,Int]()
  val mh:Int =  model.mh
  var error = false

  def wordSample(pkey: PartitionKey, csr: PartCSRResult): Unit = {
    val ws: Int = pkey.getStartRow
    val we: Int = pkey.getEndRow
    val rand: Random = new Random(System.currentTimeMillis)
    var w: Int = ws
    while (w < we){
      if (!csr.read(wk,false)) {
        throw new AngelException("some error happens")
      }
      val update: DenseIntVector = new DenseIntVector(K)
      var wi: Int = data.ws(w)
      while ( wi < data.ws(w + 1) ) {
        breakable {
          var tt: Int = data.topics(wi)
          if (wk(tt) <= 0) {
            Sampler.LOG.error(s"Error wk[$tt] = ${wk(tt)} for word $w")
            error = true
            break
          }
          wk(tt) -= 1
          nk(tt) -= 1
          update.plusBy(tt, -1)


          var s: Int = tt
          var t: Int = 0
          var pai: Float = 1f
          val steps = math.min(data.mhSteps(wi).toInt, mh)
          (0 until steps) foreach { i =>
            breakable{
              t = data.mhProp(i)(wi)
              if(wk(s) < 0 || wk(t) < 0) {
                break
              }
              pai = math.min(1f, (wk(t) + beta) * (nk(s) + vbeta) / ((wk(s) + beta) * (nk(t) + vbeta)))
              if (rand.nextFloat() < pai) tt = t
              s = t
            }
          }

          wk(tt) += 1
          nk(tt) += 1
          data.topics(wi) = tt
          data.mhSteps(wi) = math.ceil(1 / pai).toByte
          update.plusBy(tt, 1)
          wi += 1
        }
      }
      model.wtMat.increment(w, update)
      w += 1
      }
    csr.reset()
    w = ws
    while(w < we) {
      if (!csr.read(wk,true)) {
        throw new AngelException("some error happens")
      }
      var aliasTable = new AliasTable(wk.zipWithIndex.filter(_._1 > 0))
      var wi: Int = data.ws(w)
      while ( wi < data.ws(w + 1) ) {
        (0 until mh) foreach{i =>
          data.mhProp(i)(wi) = aliasTable.apply()
        }
        wi += 1
      }
      aliasTable = null
      w += 1
    }
  }

  def docSample(d: Int):Unit = {
    val rand: Random = new Random(System.currentTimeMillis)
    docTopicCount(d)
    val len = data.docLens(d)
    var di: Int = data.accDoc(d)
    while(di < data.accDoc(d + 1)){
      breakable{
        val wi = data.inverseMatrix(di)
        var tt = data.topics(wi)
        if (dk(tt) <= 0) {
          Sampler.LOG.error(s"Error nk[$tt] = ${nk(tt)} for doc $d")
          error = true
          break
        }
        dk(tt) -= 1
        nk(tt) -= 1

        var s: Int = tt
        var t: Int = 0
        var pai: Float = 1f
        val steps = math.min(data.mhSteps(wi).toInt, mh)

        (0 until steps) foreach { i =>
          t = data.mhProp(i)(wi)
          pai = math.min(1f, (dk(t) + alpha) * (nk(s) + vbeta) / ((dk(s) + alpha) * (nk(t) + vbeta)))
          if (rand.nextFloat() < pai) tt = t
          s = t
        }
        dk(tt) += 1
        nk(tt) += 1
        data.topics(wi) = tt

        (0 until mh) foreach{ i=>
          data.mhProp(i)(wi) = if(rand.nextFloat < len / (len + dalpha)){
              data.topics(data.inverseMatrix(d + rand.nextInt(len)))
          } else rand.nextInt(K)
        }
        di += 1
      }
    }
  }


  def initialize(pkey: PartitionKey): Unit = {
    val ws: Int = pkey.getStartRow
    val we: Int = pkey.getEndRow
    val rand: Random = new Random(System.currentTimeMillis)
    var w: Int = ws
    while (w < we) {
      val update: DenseIntVector = new DenseIntVector(K)
      var wi: Int = data.ws(w)
      while (wi < data.ws(w + 1)) {
        val t: Int = rand.nextInt(K)
        data.topics(wi) = t
        nk(t) += 1
        update.plusBy(t, 1)
        (0 until mh) foreach {i=>
          data.mhProp(i)(wi) = rand.nextInt(K)
        }
          wi += 1
      }
      model.wtMat.increment(w, update)
      w += 1
    }
  }

  def docTopicCount(d:Int):Unit = {
    dk.empty
    (data.accDoc(d) until data.accDoc(d + 1)) foreach{i =>
      val k = data.topics(data.inverseMatrix(i))
      dk += k -> (dk.getOrElse(k, 0) + 1)
    }
  }

  def set(nk: Array[Int]): Sampler = {
    System.arraycopy(nk, 0, this.nk, 0, K)
    this
  }

  def reset(pkey: PartitionKey): Unit = {
    val ws: Int = pkey.getStartRow
    val es: Int = pkey.getEndRow
    var w: Int = ws
    while (w < es) {
      val update: DenseIntVector = new DenseIntVector(K)
      var wi: Int = data.ws(w)
      while (wi < data.ws(w + 1)) {
        val tt: Int = data.topics(wi)
        update.plusBy(tt, 1)
        nk(tt) += 1
          wi += 1
      }
      model.wtMat.increment(w, update)
      w += 1
    }
  }

  def initForInference(pkey: PartitionKey): Unit = {
    val ws: Int = pkey.getStartRow
    val es: Int = pkey.getEndRow
    val rand: Random = new Random(System.currentTimeMillis)
    var w: Int = ws
    while (w < es) {
      var wi: Int = data.ws(w)
      while (wi < data.ws(w + 1)) {
        val t: Int = rand.nextInt(K)
        data.topics(wi) = t
          wi += 1
        (0 until mh) foreach {i=>
          data.mhProp(i)(wi) = rand.nextInt(K)
        }
      }
        w += 1
    }
  }

  def wordInference(pkey: PartitionKey, csr: PartCSRResult): Unit = {
    val ws: Int = pkey.getStartRow
    val we: Int = pkey.getEndRow
    val rand: Random = new Random(System.currentTimeMillis)
    var w: Int = ws
    while (w < we){
      if (!csr.read(wk,true)) {
        throw new AngelException("some error happens")
      }
      val aliasTable = new AliasTable(wk.zipWithIndex.filter(_._1 > 0))
      var wi: Int = data.ws(w)
      while ( wi < data.ws(w + 1) ) {
        breakable {
          var tt: Int = data.topics(wi)
          if (wk(tt) <= 0) {
            Sampler.LOG.error(s"Error wk[$tt] = ${wk(tt)} for word $w")
            error = true
            break
          }
          wk(tt) -= 1
          nk(tt) -= 1
          var s: Int = tt
          var t: Int = 0
          var pai: Float = 1f
          val steps = math.min(data.mhSteps(wi).toInt, mh)
          (0 until steps) foreach { i =>
            breakable{
              t = data.mhProp(i)(wi)
              if(wk(t) < 0 || wk(s) < 0) {
                break
              }
              pai = math.min(1f, (wk(t) + beta) * (nk(s) + vbeta) / ((wk(s) + beta) * (nk(t) + vbeta)))
              if (rand.nextFloat() < pai) tt = t
              s = t
            }
          }

          wk(tt) += 1
          nk(tt) += 1

          data.topics(wi) = tt
          data.mhSteps(wi) = math.ceil(1 / pai).toByte
          (0 until mh) foreach{i =>
            data.mhProp(i)(wi) = aliasTable.apply()
          }
          wi += 1
        }
      }
      w += 1
    }
  }
}
