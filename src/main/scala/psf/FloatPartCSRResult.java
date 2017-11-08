package psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerSparseFloatRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Created by chris on 9/19/17.
 */
public class FloatPartCSRResult extends PartitionGetResult{
    private static final Log LOG = LogFactory.getLog(FloatPartCSRResult.class);

    private List<ServerRow> splits;
    private ByteBuf buf;
    private int len;
    private int readerIdx;

    public FloatPartCSRResult(List<ServerRow> splits) {
        this.splits = splits;
    }

    public FloatPartCSRResult() {}

    @Override
    public void serialize(ByteBuf buf) {
        // Write #rows
        buf.writeInt(splits.size());
        // Write each row
        for (ServerRow row : splits) {
            if (row instanceof ServerSparseFloatRow) {
                serialize(buf, (ServerSparseFloatRow) row);
            } else {
                throw new AngelException("LDA should be set with ServerDenseIntRow");
            }
        }
    }

    public void serialize(ByteBuf buf, ServerSparseFloatRow row) {

        try {
            row.getLock().readLock().lock();
            Int2FloatOpenHashMap map = row.getData();
            int cnt = 0;

            for (Map.Entry<Integer, Float> entry : map.entrySet()) {
                if(entry.getValue() != 0.0) {
                    cnt ++;
                }
            }


            buf.writeInt(cnt);
            for (Map.Entry<Integer, Float> entry : map.entrySet()) {
                if(entry.getValue() != 0.0) {
                    buf.writeInt(entry.getKey());
                    buf.writeFloat(entry.getValue());
                }
            }
        } finally {
            row.getLock().readLock().unlock();
        }

    }

    @Override
    public void deserialize(ByteBuf buf) {
        this.len = buf.readInt();
        this.buf = buf.duplicate();
        this.buf.retain();
//    LOG.info(buf.refCnt());
        this.readerIdx = 0;
    }

    @Override
    public int bufferLen() {
        return 48;
    }

    public boolean read(float[] row) {
        if (readerIdx == len)
            return false;

        readerIdx ++;
                // sparse

        Arrays.fill(row, 0f);
        int len = buf.readInt();
        for (int i = 0; i < len; i ++) {
            int key = buf.readInt();
            float val = buf.readFloat();
            row[key] = val;
        }

        if (readerIdx == this.len) {
            buf.release();
//      LOG.info(buf.refCnt());
        }
        return true;
    }

}
