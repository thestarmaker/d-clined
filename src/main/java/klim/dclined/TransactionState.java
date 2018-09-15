/*
 * Copyright (C) 2018 Michail Klimenkov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package klim.dclined;

import io.dgraph.DgraphProto;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Instances of this class represent state of any transaction.
 *
 * @author Michail Klimenkov
 */
public class TransactionState {
    private final long startTs;
    private final Set<String> keys;
    private final Set<String> preds;

    public TransactionState() {
        this(0, emptySet(), emptySet());
    }

    public TransactionState(long startTs, Set<String> keys, Set<String> preds) {
        this.startTs = startTs;
        this.keys = keys;
        this.preds = preds;
    }

    public long getStartTs() {
        return startTs;
    }

    public Set<String> getKeys() {
        return keys;
    }

    public Set<String> getPreds() {
        return preds;
    }

    /**
     * Merges the provided context into this transaction state instance.
     *
     * @param context
     * @return
     */
    public TransactionState mergeContext(DgraphProto.TxnContext context) {
        long freshStartTs;
        if (startTs == 0) {
            freshStartTs = context.getStartTs();
        } else if (startTs != context.getStartTs()) {
            throw new IllegalStateException(String.format("startTs mismatch: localStartTs=%s, responseContextStartTs=%s", startTs, context.getStartTs()));
        } else {
            freshStartTs = startTs;
        }

        Set<String> freshKeys = new HashSet<>(context.getKeysList());
        freshKeys.addAll(keys);

        Set<String> freshPreds = new HashSet<>(context.getPredsList());
        freshPreds.addAll(preds);

        return new TransactionState(freshStartTs, freshKeys, freshPreds);
    }
}
