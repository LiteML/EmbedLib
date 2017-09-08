package embed.lda.warplda;

/**
 * Created by chris on 9/8/17.
 */
public class JDocument {
    public int docId;
    public int len;
    public int[] wids;

    public JDocument(int docId, int[] wids) {
        this.docId = docId;
        this.len = wids.length;
        this.wids = wids;
    }

    public JDocument(String str) {
        if (str.length() == 0)
            return;

        String[] parts = str.split("\t");
        docId = Integer.parseInt(parts[0]);
        String wordIds = parts[1];

        String[] splits = wordIds.split(" ");
        if (splits.length < 1)
            return;

        wids = new int[splits.length];
        for (int i = 0; i < splits.length; i++)
            wids[i] = Integer.parseInt(splits[i]);

        len = splits.length;
    }

    public int len() {
        return len;
    }
}
