package embed.randP

import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.data.inputformat.BalanceInputFormat

/**
  * Created by chris on 9/19/17.
  */
class RRunner extends MLRunner {
  private val LOG = LogFactory.getLog(classOf[RRunner])

  override
  def train(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)
    LOG.info(s"n_tasks=${conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 0)}")
    //    train(conf, new LDAModel(conf), classOf[LDATrainTask])

    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new RModel(conf))
    client.runTask(classOf[RTrainTask])
    client.waitForCompletion()
    //    client.saveModel(model)
    client.stop()
  }

  override
  def predict(conf: Configuration): Unit = ???
  override def incTrain(conf: Configuration): Unit = ???

}
