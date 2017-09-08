package embed.lda.warplda

import java.io.{BufferedReader, InputStreamReader}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import embed.lda.LDAModel
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import java.util
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/8/17.
  */
class JLDATrainTask(val ctx: TaskContext) extends
  BaseTask[LongWritable, Text, Text](ctx) {

  val LOG = LogFactory.getLog(classOf[LDAInferTask])

  override
  def parse(key: LongWritable, value: Text): Text = { null }

  override
  def preProcess(ctx: TaskContext) {
    // do nothing
  }

  @throws[Exception]
  def run(ctx: TaskContext): Unit = {
    val reader = ctx.getReader[LongWritable, Text]
    val docs  = new util.ArrayList[JDocument]()
    var did = 0
    var N = 0
    while (reader.nextKeyValue()) {
      val doc  = new JDocument(reader.getCurrentValue.toString)
      docs.add(doc)
      did += 1
      N += doc.len()
    }
    val model = new LDAModel(ctx.getConf, ctx)

    LOG.info(s"V=${model.V} K=${model.K} alpha=${model.alpha} "
      + s"beta=${model.beta} M=${docs.size} tokens=$N "
      + s"threadNum=${model.threadNum} mh=${model.mh}")

    val data = new JWTokens(model.V, docs.size())
    data.build(docs, model.K, model.mh)
    docs.clear()


    // training
    val learner = new JTrainer(ctx, model, data)
    learner.initialize()
  }

  def loadModel(model: LDAModel): Unit = {
    val paths = getPaths()
    val update = new DenseIntVector(model.K)

    for (i <- 0 until paths.length) {
      val path = paths(i)
      LOG.info(s"Load model from path ${path}")
      val fs   = path.getFileSystem(conf)

      val in = new BufferedReader(new InputStreamReader(fs.open(path)))

      var finish = false
      while (!finish) {
        in.readLine() match {
          case line: String =>
            val parts = line.split(": ")
            val topics = parts(1).split(" ")
            val vector = new DenseIntVector(model.K)
            for (i <- 0 until model.K) {
              vector.set(i, topics(i).toInt)
              update.plusBy(i, topics(i).toInt)
            }
            model.wtMat.increment(parts(0).toInt, vector)
          case null => finish = true
        }
      }

      in.close()
    }

    model.tMat.increment(0, update)
    model.wtMat.clock().get()
    model.tMat.clock().get()
  }

  def getPaths(): Array[Path] = {
    val taskId = ctx.getTaskIndex
    val total  = ctx.getTotalTaskNum
    val dir    = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    val base   = dir + "/" + "word_topic"

    val basePath = new Path(base)
    val fs = basePath.getFileSystem(conf)
    if (!fs.exists(basePath))
      throw new AngelException(s"Model load path does not exist ${base}")

    if (!fs.isDirectory(basePath))
      throw new AngelException(s"Model load path ${base} is not a directory")

    val statuses = fs.listStatus(basePath)
    val ret = new ArrayBuffer[Path]()
    for (i <- 0 until statuses.length) {
      val status = statuses(i)
      if (status.getPath != null && i % total == taskId)
        ret.append(status.getPath)
    }

    ret.toArray
  }

}
