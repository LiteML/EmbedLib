package appAdni.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chris on 12/7/17.
 */
public class PartialParam extends GetParam{
    public static class PartialPartParam extends PartitionGetParam {
        private int[] ids;
        public PartialPartParam(int matrixId, PartitionKey pkey, int[] ids) {
            super(matrixId,pkey);
            this.ids = ids;
        }
        public PartialPartParam(){super();}

        @Override
        public void serialize(ByteBuf buf) {
            super.serialize(buf);
            buf.writeInt(ids.length);
            for(int id:ids) {
                buf.writeInt(id);
            }
        }

        @Override
        public void deserialize(ByteBuf buf) {
            super.deserialize(buf);
            int idLen = buf.readInt();
            ids = new int[idLen];
            for(int i = 0; i < idLen; i++) {
                ids[i] = buf.readInt();
            }
        }

        @Override
        public int bufferLen() {
            return super.bufferLen() + 8 * ids.length;
        }

        public int[] getIds() {return ids;}

    }

    private final int[] ids;
    public PartialParam(int matrixId, int[] ids) {
        super(matrixId);
        this.ids = ids;
    }

    @Override
    public List<PartitionGetParam> split() {
        List<PartitionKey> parts =
                PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
        int size = parts.size();
        List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);
        for (PartitionKey part : parts) {
            partParams.add(new PartialPartParam(matrixId,part, ids));
        }
        return partParams;

    }

}
