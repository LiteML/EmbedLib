package psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class GetFloatPartFunc extends GetFunc {

  private final static Log LOG = LogFactory.getLog(GetFloatPartFunc.class);

  public GetFloatPartFunc(GetParam param) {
    super(param);
  }

  public GetFloatPartFunc() { super(null);}

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {

    PartitionKey pkey = partParam.getPartKey();
    pkey = PSContext.get().getMatrixPartitionManager()
            .getPartition(pkey.getMatrixId(), pkey.getPartitionId())
            .getPartitionKey();
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();


    MatrixPartitionManager manager = PSContext.get().getMatrixPartitionManager();
    List<ServerRow> rows = new ArrayList<>();
    for (int w = ws; w < es ; w ++)
      rows.add(manager.getRow(pkey, w));

    FloatPartCSRResult csr = new FloatPartCSRResult(rows);
    return csr;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    return null;
  }
}
