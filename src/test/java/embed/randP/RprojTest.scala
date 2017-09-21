package embed.randP

import com.tencent.angel.client.{AngelClient, AngelClientFactory}
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.PropertyConfigurator
import org.junit.{Before, Test}
import embed.randP.RModel._
/**
  * Created by chris on 9/20/17.
  */
class RprojTest {
  private val conf: Configuration = new Configuration
  private val LOG: Log = LogFactory.getLog(classOf[RprojTest])
  private val LOCAL_FS: String = FileSystem.DEFAULT_FS
  private val TMP_PATH: String = System.getProperty("java.io.tmpdir", "/tmp")
  private var client: AngelClient = null
  PropertyConfigurator.configure("./src/conf/log4j.properties")
  LOG.info(System.getProperty("user.dir"))
  @Before
  def setup(): Unit = {
    val inputPath: String = "./src/test/data/RProj/rand.train"

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[RTrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input and output path
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/LOG/rplog")
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1)

    client = AngelClientFactory.get(conf)

    val F = 100
    val C = 10
    val S = 10
    val B = 10

    conf.setInt(PARAM_S, S)
    conf.setInt(FEATURE_NUM, F)
    conf.setInt(COMPONENTS_NUM, C)
    conf.setInt(MLConf.ML_WORKER_THREAD_NUM, 2)
    conf.setInt(BATCH_SIZE,B)
    conf.setInt(MLConf.ML_EPOCH_NUM, 0)
    conf.setBoolean(SAVE_MAT, true)
  }

  @Test
  def run(): Unit = {
    //start PS
    client.startPSServer()

    //init model
    val model = new RModel(conf)

    // Load model meta to client
    client.loadModel(model)

    // Start
    client.runTask(classOf[RTrainTask])

    // Run user task and wait for completion, user task is set in "angel.task.user.task.class"
    client.waitForCompletion()

    // Save the trained model to HDFS
    //    client.saveModel(lDAModel)

    client.stop()
  }
}
