package schain

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.conf.AngelConf.ANGEL_PS_NUMBER
import com.tencent.angel.ml.conf.MLConf.{DEFAULT_ML_PART_PER_SERVER, DEFAULT_ML_WORKER_THREAD_NUM, ML_PART_PER_SERVER, ML_WORKER_THREAD_NUM}
import com.tencent.angel.ml.math.vector.SparseFloatVector
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import embed.randP.RModel.PSBATCH_SIZE
import schain.SCModel._

/**
  * Created by chris on 9/20/17.
  */
object SCModel{
  val RAND_MAT = "rand_matrix"

  val MAT_ONE = "mat_one"

  val MAT_TWO = "mat_two"

  val PARAM_S="ml.schain.s"

  val MATDIM_ONE="ml.schain.Dmat1"

  val MATDIM_TWO="ml.schain.Dmat2"

  val SAMPLE = "ml.schain.sampleSize"



  //Dimension after reduction
  val COMPONENTS_NUM = "ml.schain.component"

  // Whether save
  val SAVE_MAT = "save.mat"

  val BATCH_SIZE = "ml.schain.batchSize"

  val PSBATCH_SIZE = "ml.schain.psBatchSize"

  val PSRANDP_SIZE = "ml.schain.psRandPSize"



}
class SCModel (conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  val R:Int = conf.getInt(COMPONENTS_NUM, 10)
  val N:Int = conf.getInt(SAMPLE,2000000)
  val S:Double = conf.getDouble(PARAM_S, 3d)
  val DIM_1:Int = conf.getInt(MATDIM_ONE,203)
  val DIM_2:Int = conf.getInt(MATDIM_TWO,5000)

  // Initializing model matrices
  val threadNum:Int = conf.getInt(ML_WORKER_THREAD_NUM, DEFAULT_ML_WORKER_THREAD_NUM)
  val psNum:Int = conf.getInt(ANGEL_PS_NUMBER, 1)
  val parts:Int = conf.getInt(ML_PART_PER_SERVER, DEFAULT_ML_PART_PER_SERVER)
  val saveMat:Boolean = conf.getBoolean(SAVE_MAT, true)
  val batchSize:Int = conf.getInt(BATCH_SIZE,1000000)
  val psBatchSize:Int = conf.getInt(PSBATCH_SIZE,10)
  val psRandPSize:Int = conf.getInt(PSRANDP_SIZE,10)


  val wtMat = PSModel[SparseFloatVector](RAND_MAT, R, N, psRandPSize, N)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(wtMat)

  val mat1 = PSModel[SparseFloatVector](MAT_ONE, N, DIM_1,psBatchSize, DIM_1)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(mat1)

  val mat2 = PSModel[SparseFloatVector](MAT_TWO, N, DIM_2, psBatchSize, DIM_2)
    .setRowType(RowType.T_FLOAT_SPARSE)
    .setOplogType("DENSE_FLOAT")
  addPSModel(mat2)




  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }

}
