package embed.lda.warplda.get;

import com.tencent.angel.PartitionKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chris on 8/25/17.
 */
public class PartDocResult {
    public static PartitionKey[] getPartitionKeyList(int Docs, int Parts) {
        PartitionKey[] keys = new PartitionKey[Parts];
        int len = Docs/Parts;
        for(int i = 0; i <= Parts; i++) {
            if(i * len == Docs) {
                break;
            }
            int endRow = Math.min((i+1)*len, Docs);
            keys[i] = new PartitionKey(i,0,i*len,0,endRow,0);
        }
        return keys;
    }
}