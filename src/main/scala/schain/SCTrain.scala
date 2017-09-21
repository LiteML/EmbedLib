package schain

import java.io.{BufferedReader, InputStreamReader}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.vector.SparseFloatVector
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/21/17.
  */
class SCTrain(val ctx: TaskContext) extends
  BaseTask[LongWritable, Text, Text](ctx) {

  val LOG = LogFactory.getLog(classOf[SCTrain])

  override
  def parse(key: LongWritable, value: Text): Text = {
    null
  }

  override
  def preProcess(ctx: TaskContext) {
    // do nothing
  }

  @throws[Exception]
  def run(ctx: TaskContext): Unit = {
    // load model
    val model = new SCModel(conf, ctx)
    val data = loadData(model)

    // load data

    val train = new SCLearner(ctx, data, model)
    train.scheduleInit()
    train.scheduleMultiply()

    // save doc_topic

  }

  def loadData(model: SCModel): SMatrix = {
    val rows = ArrayBuffer[SRow]()
    val paths = getPaths(model)
    paths.indices.foreach { i =>
      val path = paths(i)
      LOG.info(s"Load model from path $path")
      val fs = path.getFileSystem(conf)
      val in = new BufferedReader(new InputStreamReader(fs.open(path)))

      var finish = false
      while (!finish) {
        in.readLine() match {
          case line: String =>
            val row = new SRow(model.DIM_1, line)
            rows.+=(row)
            val parts = line.split("\t")
            val features = parts(1).split(" ").map { f =>
              (Integer.parseInt(f.split(":")(0)), f.split(":")(1).toFloat)
            }
            val vector1 = new SparseFloatVector(model.DIM_1)
            val vector2 = new SparseFloatVector(model.DIM_2)
            features.foreach { case (k, v) =>
              if (k < model.DIM_1) {
                vector1.set(k, v)
              } else {
                vector2.set(k - model.DIM_1, v)
              }
            }
            model.mat1.increment(Integer.parseInt(parts(0)), vector1)
            model.mat2.increment(Integer.parseInt(parts(1)), vector2)
          case null => finish = true
        }
      }
      in.close()
    }
    model.mat1.clock().get()
    model.mat2.clock().get()
    val data = new SMatrix(rows.length)
    data.build(rows)
    rows.clear()
    data
  }

  def getPaths(model: SCModel): Array[Path] = {
    val taskId = ctx.getTaskIndex
    val total = ctx.getTotalTaskNum
    val pathMat = model.pathMat

    val basePath = new Path(pathMat)
    val fs = basePath.getFileSystem(conf)
    if (!fs.exists(basePath))
      throw new AngelException(s"Matrix load path does not exist $basePath")

    if (!fs.isDirectory(basePath))
      throw new AngelException(s"Matrix load path $basePath is not a directory")

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
