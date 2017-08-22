package embed.tmp

/**
  * Created by chris on 8/22/17.
  */
case class Document(docId:Long, len:Int, wids:Array[Int])


class WTokens (n_words:Int, n_docs:Int)  {
  var ws:Array[Int] = _
  var docs:Array[Int] = _  // give word return doc
  var topics:Array[Int] = _
  var docLens:Array[Int] = _
  var docIds:Array[Long] = _
  var mhProp:Array[Int] = _
  var n_tokens:Int = _
  var inverseMatrix:Array[Int] = _
  var accDoc:Array[Int] = _
  var mhSteps:Array[Byte] = _


  def build(docs:Array[Document],K:Int,mh:Int) = {
    val wcnt = Array.ofDim[Int](n_words)
    this.ws = Array.ofDim[Int](n_words + 1)
    this.accDoc = Array.ofDim[Int](n_docs + 1)
    docLens = Array.ofDim[Int](n_docs)
    docIds = Array.ofDim[Long](n_docs)
    n_tokens = 0
    (0 until docs.length) foreach {d=>
      val doc = docs(d)
      (0 until doc.len) foreach {w =>
        wcnt(doc.wids(w)) += 1
        n_tokens += doc.len
        docLens(d) = doc.len
        docIds(d) = doc.docId
      }
    }
    this.topics = Array.ofDim[Int](n_tokens)
    this.mhSteps = Array.ofDim[Byte](n_tokens)
    this.inverseMatrix = Array.ofDim[Int](n_tokens)
    //word count
    ws(0) = 0
    (0 until n_words + 1) foreach{ i=>
      ws(i+1) = ws(i) + wcnt(i)
    }
    this.topics = Array.ofDim[Int](n_tokens)
    this.mhProp = Array.ofDim[Int](mh*n_tokens)
    //doc count
    accDoc(0) = 0
    (0 until n_docs + 1) foreach { i=>
      accDoc(i+1) = accDoc(i) + docLens(i)
    }

    var start = n_tokens - 1
    (n_docs until 0 by -1) foreach{d =>
      val doc = docs(d - 1)
      (doc.len until 0 by -1) foreach{ w =>
        val wid = doc.wids(w - 1)
        inverseMatrix(start - w + 1) = ws(wid) + {
          wcnt(wid) -= 1
          wcnt(wid)
        }
          start += 1
      }
    }
  }
}
