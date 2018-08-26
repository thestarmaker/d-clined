package klim.dclined;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.TxnContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static klim.dclined.Helpers.mergeIdMaps;

/**
 * @author Michail Klimenkov
 */
public class DClined extends AbstractClient {
    private final DgraphStub stub;

    private Map<Integer, Long> idsMap = Collections.emptyMap();

    public DClined(DgraphStub stub) {
        this.stub = stub;
    }

    public Transaction newTransaction() {
        return new Transaction(stub, this::updateIdsMap, idsMap);
    }

    private synchronized void updateIdsMap(Map<Integer, Long> update) {
        this.idsMap = mergeIdMaps(update, idsMap);
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
        return new TransactionState(idsMap);
    }

    @Override
    protected DgraphStub getStub() {
        return stub;
    }

    @Override
    protected void mergeContext(TxnContext conext) {
        updateIdsMap(conext.getLinRead().getIdsMap());
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
}
