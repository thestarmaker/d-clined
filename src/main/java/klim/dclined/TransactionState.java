package klim.dclined;

import io.dgraph.DgraphProto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static klim.dclined.Helpers.mergeIdMaps;

/**
 * @author Michail Klimenkov
 */
public class TransactionState {
    private final Map<Integer, Long> idsMap;
    private final long startTs;
    private final List<String> keys;
    private final List<String> preds;

    public TransactionState(Map<Integer, Long> idsMap) {
        this(idsMap, 0, emptyList(), emptyList());
    }

    public TransactionState(Map<Integer, Long> idsMap, long startTs, List<String> keys, List<String> preds) {
        this.idsMap = idsMap;
        this.startTs = startTs;
        this.keys = keys;
        this.preds = preds;
    }

    public Map<Integer, Long> getIdsMap() {
        return idsMap;
    }

    public long getStartTs() {
        return startTs;
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<String> getPreds() {
        return preds;
    }

    public TransactionState mergeContext(DgraphProto.TxnContext context) {
        Map<Integer, Long> freshIdsMap = mergeIdMaps(context.getLinRead().getIdsMap(), idsMap);

        long freshStartTs;
        if (startTs == 0) {
            freshStartTs = context.getStartTs();
        } else if (startTs != context.getStartTs()) {
            throw new IllegalStateException(String.format("startTs mismatch: localStartTs=%s, responseContextStartTs=%s", startTs, context.getStartTs()));
        } else {
            freshStartTs = startTs;
        }

        //TODO wtf is this?? is it really additive list? Maybe Set?
        List<String> freshKeys = new ArrayList<>(context.getKeysList());
        freshKeys.addAll(keys);

        List<String> freshPreds = new ArrayList<>(context.getPredsList());
        freshPreds.addAll(preds);

        return new TransactionState(freshIdsMap, freshStartTs, freshKeys, freshPreds);
    }
}
