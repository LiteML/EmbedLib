package embed.lda.mh

import embed.lda.LDAConfig
import java.util.Random

import breeze.linalg.Vector
import embed.sampling.AliasTable

/**
  * A metropolis hastings sampler based on the WarpLDA implementation
  * Created by chris on 8/10/17.
  */
class Sampler(config:LDAConfig, mhSteps:Int, r:Random) {
  private val α = config.α
  private val β = config.β
  private val αSum = config.topics * α
  private val βSum = config.vocabularyTerms * config.β
  private val K = config.topics

  var infer: Int = 1
  var aliasTable: AliasTable = null
  var wordCounts: Array[Int] = null
  var globalCounts: Array[Long] = null
  var documentCounts: Array[Int] = null
  var documentTopicAssignments: Array[Int] = null
  var documentSize: Int = 0

  def sample(oldTopic:Int):Int = {
    var s: Int = oldTopic
    (0 until mhSteps) foreach{i =>
      //Word Proposal
      var t:Int = s
      t = aliasTable.apply()
      if (t != s) {
        var docS = documentCounts(s) + α
        var docT = documentCounts(t) + α
        var globalS = globalCounts(s) + βSum
        var globalT = globalCounts(t) + βSum

        if (s == oldTopic) {
          docS -= 1
          globalS -= infer
        }
        if (t == oldTopic) {
          docT -= 1
          globalT -= infer
        }
        val a = Math.min(1f, (docT*globalS)/(docS*globalT))
        if(r.nextFloat() < a){
          s = t
        }
      }

      //Doc Proposal
      if(r.nextFloat < documentSize / (documentSize + αSum)){
        t = documentTopicAssignments(r.nextInt(K))
      } else {
        t = r.nextInt(K)
      }
      if (t != s) {
        var wordS = wordCounts(s) + β
        var wordT = wordCounts(t) + β
        var globalS = globalCounts(s) + βSum
        var globalT = globalCounts(t) + βSum

        if (s == oldTopic) {
          wordS -= infer
          globalS -= infer
        }
        if (t == oldTopic) {
          wordT -= infer
          globalT -= infer
        }
        val a = Math.min(1f, (wordT*globalS)/(wordS*globalT))
        if(r.nextFloat() < a){
          s = t
        }
      }
    }
    s
  }
}
