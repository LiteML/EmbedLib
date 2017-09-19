package embed.randP

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/19/17.
  */
class RTrainTask(val ctx:TaskContext) extends BaseTask[LongWritable,Text,Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[RTrainTask])

  override
  def parse(key_in: LongWritable, value_in: Text): Text = ???

  override
  def preProcess(taskContext: TaskContext): Unit = {}

  @throws[Exception]
  def run(ctx:TaskContext) :Unit = {
    val reader = ctx.getReader[LongWritable,Text]
    var rows = new ArrayBuffer[Row]()
    var did = 0
    var N = 0
    while(reader.nextKeyValue()) {
      val text = reader.getCurrentValue
      val row = new Row(text.toString)
      if(row != null) {
        rows.+= (row)
        did += 1
        N += row.len
      }
    }
    reader.close()

    val model = new RModel(ctx.getConf,ctx)

    LOG.info(s"Feature=${model.F} Components=${model.R} "
      + s"S=${model.S} Batch=${model.batchSize} Entries=$N "
      + s"threadNum=${model.threadNum}")

    val data = new Matrix(rows.length)
    data.build(rows)
    rows.clear()

    val learner = new RLeaner(ctx,model,data)
    learner.scheduleInit()
    learner.scheduleMultiply()
  }
}
