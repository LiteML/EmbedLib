package embed.lda.warplda

import java.io.{BufferedReader, InputStreamReader}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/1/17.
  */
class LDAInferTask(val ctx: TaskContext) extends
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
    // load model
    val model = new LDAModel(conf, ctx)
    loadModel(model)

    // load data
    val data = read(model.V, model.K,model.mh)

    val infer = new Trainer(ctx, model, data)
    infer.initForInference()
    infer.inference(model.epoch)

    // save doc_topic

    if (model.saveDocTopic) infer.saveDocTopic(data, model)
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

  def read(V: Int, K: Int, mh:Int): WTokens = {
    // Read documents
    val reader = ctx.getReader[LongWritable, Text]
    val docs   = new ArrayBuffer[Document]()
    var did = 0
    var N = 0
    while (reader.nextKeyValue()) {
      val doc  = new Document(reader.getCurrentValue.toString)
      docs.+=(doc)
      did += 1
      N += doc.len
    }
    reader.close()

    val data = new WTokens(V, docs.length)
    data.build(docs, K, mh)
    docs.clear()
    data
  }
}
