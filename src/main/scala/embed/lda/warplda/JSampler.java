package embed.lda.warplda;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import embed.lda.LDAModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Random;

/**
 * Created by chris on 9/8/17.
 */
public class JSampler {
    JWTokens data;
    LDAModel model;
    int K;
    float alpha, beta, vbeta;
    int[] nk;
    int[] wk;
    private final static Log LOG = LogFactory.getLog(Sampler.class);



    public JSampler(JWTokens data, LDAModel model) {
        this.data = data;
        this.model = model;
        K = model.K();
        alpha = model.alpha();
        beta = model.beta();
        nk = new int[K];
        wk = new int[K];
    }

    public void initialize(PartitionKey pkey) {
        int ws = pkey.getStartRow();
        int es = pkey.getEndRow();
        Random rand = new Random(System.currentTimeMillis());
        for(int w = ws; w < es; w ++) {
            DenseIntVector update = new DenseIntVector(K);
            for(int wi = data.ws[w]; wi < data.ws[w + 1]; w ++){
                int t  = rand.nextInt(K);
                data.topics[wi] = t;
                nk[t] += 1;
                update.plusBy(t, 1);
                for(int i = 0; i < model.mh(); i ++) {
                    data.mhProp[i][wi] = rand.nextInt(K);
                }
            }
            model.wtMat().increment(w, update);
            }
        }
    }

