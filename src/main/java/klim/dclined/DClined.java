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
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.TxnContext;
import io.grpc.ManagedChannel;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * @author Michail Klimenkov
 */
public class DClined extends AbstractClient implements Closeable {
    private final ManagedChannel channel;
    private final DgraphStub stub;

    public DClined(ManagedChannel channel) {
        this.channel = channel;
        this.stub = DgraphGrpc.newStub(channel);
    }

    public Transaction newTransaction() {
        return new Transaction(stub);
    }

    private CompletableFuture<Void> alter(Operation op) {
        StreamObserverBridge<DgraphProto.Payload> observerBridge = new StreamObserverBridge<>();
        getStub().alter(op, observerBridge);
        return observerBridge.getDelegate().thenApply((p) -> null);
    }

    public CompletableFuture<Void> dropAll() {
        Operation operation = Operation.newBuilder()
                .setDropAll(true)
                .build();

        return alter(operation);
    }

    public CompletableFuture<Void> schema(String schema) {
        Operation operation = Operation.newBuilder()
                .setSchema(schema)
                .build();

        return alter(operation);
    }

    public CompletableFuture<Void> dropAttribute(String command) {
        Operation operation = Operation.newBuilder()
                .setDropAttr(command)
                .build();

        return alter(operation);
    }


    @Override
    protected TransactionState getState() {
        return new TransactionState();
    }

    @Override
    protected DgraphStub getStub() {
        return stub;
    }

    @Override
    protected void mergeContext(TxnContext conext) {
        //do nothing
    }

    @Override
    protected Mutation newMutation(BiConsumer<Mutation.Builder, NQuad> aggregator, NQuad... nQuads) {
        Mutation.Builder builder = Mutation.newBuilder();
        for (NQuad nQuad : nQuads) {
            aggregator.accept(builder, nQuad);
        }

        return builder.setCommitNow(true).build();
    }

    @Override
    protected Mutation newMutation(BiConsumer<Mutation.Builder, ByteString> stringNQuadSetter, String nQuads) {
        Mutation.Builder builder = Mutation.newBuilder();
        stringNQuadSetter.accept(builder, ByteString.copyFromUtf8(nQuads));
        return builder.setCommitNow(true).build();
    }

    @Override
    public void close() {
        LOG.info("Shutting down...");
        channel.shutdown();
    }
}
