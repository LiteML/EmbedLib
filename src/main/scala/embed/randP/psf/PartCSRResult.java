package embed.randP.psf;

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
public class PartCSRResult extends PartitionGetResult{
    private static final Log LOG = LogFactory.getLog(PartCSRResult.class);

    private List<ServerRow> splits;
    private ByteBuf buf;
    private int len;
    private int readerIdx;

    public PartCSRResult(List<ServerRow> splits) {
        this.splits = splits;
    }

    public PartCSRResult() {}

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

            int cnt = map.size();
            buf.writeShort(cnt);
            for (Map.Entry<Integer, Float> entry : map.entrySet()) {
                buf.writeShort(entry.getKey());

                buf.writeFloat(entry.getValue());
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
        return 32;
    }

    public boolean read(float[] row) {
        if (readerIdx == len)
            return false;

        readerIdx ++;
                // sparse

        Arrays.fill(row, 0f);
        int len = buf.readShort();
        for (int i = 0; i < len; i ++) {
            int key = buf.readShort();
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
