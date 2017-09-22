package embed.schain

import com.tencent.angel.client.{AngelClient, AngelClientFactory}
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import embed.randP.{RTrainTask, RprojTest}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.PropertyConfigurator
import org.junit.{Before, Test}
import schain.{SCModel, SCTrain}
import schain.SCModel._
/**
  * Created by chris on 9/21/17.
  */
class SchainTest {
  private val conf: Configuration = new Configuration
  private val LOG: Log = LogFactory.getLog(classOf[RprojTest])
  private val LOCAL_FS: String = FileSystem.DEFAULT_FS
  private val TMP_PATH: String = System.getProperty("java.io.tmpdir", "/tmp")
  private var client: AngelClient = null
  PropertyConfigurator.configure("./src/conf/log4j.properties")
  LOG.info(System.getProperty("user.dir"))
  @Before
  def setup(): Unit = {
    val inputPath: String = "./src/test/data/Schain/schain.train"

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[RTrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input and output path
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/LOG/sclog")
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1)

    client = AngelClientFactory.get(conf)

    val Dim1 = 50
    val Dim2 = 150
    val S = 10
    val B = 100
    val C = 50
    val N = 2500

    conf.setInt(SAMPLE, 2500)
    conf.setInt(PARAM_S, S)
    conf.setInt(MATDIM_ONE,Dim1)
    conf.setInt(MATDIM_TWO, Dim2)
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
    val model = new SCModel(conf)

    // Load model meta to client
    client.loadModel(model)

    client.runTask(classOf[SCTrain])

    client.waitForCompletion()

    client.stop()
  }

}
