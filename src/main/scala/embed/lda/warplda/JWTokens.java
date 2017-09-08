package embed.lda.warplda;

import java.util.List;

/**
 * Created by chris on 9/8/17.
 */
public class JWTokens {
    public int n_words;
    public int n_docs;
    public int n_tokens;
    public int[] topics;
    public int[] ws;
    public int[][] mhProp;
    public int[] inverseMatrix;
    public int[] accDoc;
    public int[] docIds;
    public int[] docLens;

    public JWTokens(int n_words, int n_docs) {
        this.n_words = n_words;
        this.n_words = n_docs;
    }

    public void build(List<JDocument> docs, int K, int mh) {
        int[] wcnt = new int[n_words]; // word count
        this.ws = new int[n_words + 1];
        docLens = new int[n_docs];
        docIds = new int[n_docs];
        this.accDoc = new int[n_docs + 1];
        n_tokens = 0;
        for (int d = 0; d < n_docs; d++) {
            JDocument doc = docs.get(d);
            for (int w = 0; w < doc.len; w++)
                wcnt[doc.wids[w]]++;
            n_tokens += doc.len;
            docLens[d] = doc.len;
            docIds[d] = doc.docId;
        }

        this.topics = new int[n_tokens];
        this.inverseMatrix = new int[n_tokens];
        this.mhProp = new int[mh][n_tokens];

        ws[0] = 0;
        for (int i = 0; i < n_words; i++)
            ws[i + 1] = ws[i] + wcnt[i];

        accDoc[0] = 0;
        for (int i = 0; i < n_docs; i++)
            accDoc[i + 1] = accDoc[i] + docLens[i];
        int start = 0;
        for (int d = 0; d < n_docs; d++) {
            JDocument doc = docs.get(d);
            for (int w = 0; w < doc.len; w++) {
                int wid = doc.wids[w];
                inverseMatrix[start] = ws[wid] + (--wcnt[wid]);
            }
        }

    }

}
