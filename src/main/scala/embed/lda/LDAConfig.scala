package embed.lda

import scala.reflect.BeanProperty

/**
  * Lda Configuration
  * Created by chris on 8/2/17.
  */
case class LDAConfig(@BeanProperty var α: Float = 0.5f,
                     @BeanProperty var β: Float = 0.0f,
                     @BeanProperty var τ: Int = 1,
                     @BeanProperty var mhSteps: Int = 2,
                     @BeanProperty var iterations: Int = 100,
                     @BeanProperty var topics: Int = 10,
                     @BeanProperty var vocabularyTerms: Int = 100000,
                     @BeanProperty var powerlawCutoff: Int = 0,
                     @BeanProperty var partitions: Int = 240,
                     @BeanProperty var blockSize: Int = 1000,
                     @BeanProperty var checkpointEvery: Int = 1,
                     @BeanProperty var seed: Int = 42) extends Serializable {

  override def toString: String = {
    s"""LDAConfig {
       |  α = $α
       |  β = $β
       |  τ = $τ
       |  mhSteps = $mhSteps
       |  iterations = $iterations
       |  topics = $topics
       |  vocabularyTerms = $vocabularyTerms
       |  powerlawCutoff = $powerlawCutoff
       |  partitions = $partitions
       |  blockSize = $blockSize
       |  checkpointEvery = $checkpointEvery
       |  seed = $seed
       |}
    """.stripMargin
  }
}