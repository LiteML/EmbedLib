package embed.randP
import com.tencent.angel.conf.AngelConf.ANGEL_PS_NUMBER
import com.tencent.angel.ml.conf.MLConf.{DEFAULT_ML_PART_PER_SERVER, DEFAULT_ML_WORKER_THREAD_NUM, ML_PART_PER_SERVER, ML_WORKER_THREAD_NUM}
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.worker.task.TaskContext
import RModel._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock

/**
  * Created by chris on 9/18/17.
  */
object RModel {
  // Project matrix
  val RAND_MAT = "rand_matrix"

  val PARAM_S="ml.randP.s"


  // Dimension of Features
  val FEATURE_NUM = "ml.randP.feature"

  //Dimension after reduction
  val COMPONENTS_NUM = "ml.randP.component"

  // Whether save
  val SAVE_MAT = "save.mat"

  val BATCH_SIZE = "ml.randP.batchSize"

}
class RModel (conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  val F = conf.getInt(FEATURE_NUM, 1)
  val R = conf.getInt(COMPONENTS_NUM, 1)
  val S = conf.getInt(PARAM_S, 3)

  // Initializing model matrices
  val threadNum = conf.getInt(ML_WORKER_THREAD_NUM, DEFAULT_ML_WORKER_THREAD_NUM)
  val psNum = conf.getInt(ANGEL_PS_NUMBER, 1)
  val parts = conf.getInt(ML_PART_PER_SERVER, DEFAULT_ML_PART_PER_SERVER)
  val saveMat = conf.getBoolean(SAVE_MAT, true)
  val batchSize = conf.getInt(BATCH_SIZE,1000000)


  val wtMat = PSModel[DenseIntVector](RAND_MAT, R, F, Math.max(1, R / psNum), F)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("SPARSE_INT")
  addPSModel(wtMat)

  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }
}
