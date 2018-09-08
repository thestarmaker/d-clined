package klim.dclined;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.LinRead;
import io.dgraph.DgraphProto.LinRead.Sequencing;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.TxnContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Michail Klimenkov
 */
public class Transaction extends AbstractClient {

    private final DgraphStub stub;
    private final Consumer<Map<Integer, Long>> idsMapUpdateListener;
    protected final AtomicReference<TransactionState> state;

    Transaction(DgraphStub stub, Consumer<Map<Integer, Long>> idsMapUpdateListener, Map<Integer, Long> idsMap) {
        this.stub = stub;
        this.idsMapUpdateListener = idsMapUpdateListener;
        this.state = new AtomicReference<>(new TransactionState(idsMap));
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


    public CompletableFuture<Void> commit() {
        TransactionState state = this.state.get();

        LinRead linRead = LinRead.newBuilder()
                .putAllIds(state.getIdsMap())
                .setSequencing(Sequencing.CLIENT_SIDE)
                .build();

        TxnContext context = TxnContext.newBuilder()
                .setStartTs(state.getStartTs())
                .addAllKeys(state.getKeys())
                .addAllPreds(state.getPreds())
                .setLinRead(linRead)
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
        idsMapUpdateListener.accept(freshState.getIdsMap());
        this.state.set(freshState);
    }
}
