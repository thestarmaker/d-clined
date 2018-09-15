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

import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.TxnContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Instances of this class represent actions that should be executed in transactional manner.
 * Any modifications performed as part of a transaction are not visible to any other transactions
 * until the givem transaction is committed.
 *
 * @author Michail Klimenkov
 */
public class Transaction extends AbstractClient {

    private final DgraphStub stub;
    protected final AtomicReference<TransactionState> state;

    Transaction(DgraphStub stub) {
        this.stub = stub;
        this.state = new AtomicReference<>(new TransactionState());
    }


    @Override
    protected Mutation newMutation(BiConsumer<Mutation.Builder, NQuad> aggregator, NQuad... nQuads) {
        Mutation.Builder builder = Mutation.newBuilder();
        for (NQuad nQuad : nQuads) {
            aggregator.accept(builder, nQuad);
        }

        TransactionState state = this.state.get();
        return builder.setStartTs(state.getStartTs()).build();
    }

    @Override
    protected Mutation newMutation(BiConsumer<Mutation.Builder, ByteString> stringNQuadSetter, String nQuads) {
        Mutation.Builder builder = Mutation.newBuilder();
        stringNQuadSetter.accept(builder, ByteString.copyFromUtf8(nQuads));

        TransactionState state = this.state.get();
        return builder.setStartTs(state.getStartTs()).build();
    }


    /**
     * Commits the given transaction making any modifications made visible to other subsequent transactions.
     *
     * @return
     */
    public CompletableFuture<Void> commit() {
        TransactionState state = this.state.get();

        TxnContext context = TxnContext.newBuilder()
                .setStartTs(state.getStartTs())
                .addAllKeys(state.getKeys())
                .addAllPreds(state.getPreds())
                .build();

        StreamObserverBridge<TxnContext> bridge = new StreamObserverBridge<>();
        stub.commitOrAbort(context, bridge);

        return bridge
                .getDelegate()
                .handle((TxnContext txnContext, Throwable throwable) -> {
                    if (throwable != null) {
                        throw launderException(throwable);
                    }
                    return null;
                });
    }

    /**
     * Aborts this transaction discarding any uncommitted modifications.
     *
     * @return
     */
    public CompletableFuture<Void> abort() {
        return abort(stub);
    }

    @Override
    protected TransactionState getState() {
        return this.state.get();
    }

    @Override
    protected DgraphStub getStub() {
        return stub;
    }

    @Override
    protected synchronized void mergeContext(TxnContext context) {
        TransactionState freshState = this.state.get().mergeContext(context);
        this.state.set(freshState);
    }
}
