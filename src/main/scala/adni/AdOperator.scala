package adni

import java.util.concurrent.ConcurrentMap

import adni.psf.{DegreePartCSRResult, FloatPartCSRResult}
import structures.CSRMatrix

import scala.collection.mutable
/**
  * Created by chris on 11/2/17.
  */
class AdOperator(csrMatrix:CSRMatrix[Float],model:AdniModel) {
  val mVec: mutable.Map[Int,Float] = mutable.Map[Int,Float]()

  def multiply(csr:FloatPartCSRResult,
               result:Array[Float],
               original:Array[Float],
               biject:Map[Int,Int]) = {

    csr.read(mVec)
    mVec.foreach{case(k, v) =>
        biject.get(k) match {
          case Some(pos) => {
            original(pos) = v
          }
          case None => _
        }
    }

    (0 until csrMatrix.numOfRows) foreach {i =>
      result(i) += csrMatrix.dotRow(mVec.toMap, i)
    }
    mVec.clear()
  }

  def degInsert(csr:DegreePartCSRResult,
               degs:ConcurrentMap[Int,Float]
               ): Unit = {
    csr.read(degs)
  }
}
