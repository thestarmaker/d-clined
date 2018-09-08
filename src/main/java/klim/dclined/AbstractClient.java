package klim.dclined;

import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.TxnContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;

/**
 * @author Michail Klimenkov
 */
public abstract class AbstractClient {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

    protected static final Gson PARSER = new Gson();

    abstract TransactionState getState();

    abstract DgraphStub getStub();

    abstract void mergeContext(TxnContext conext);

    abstract Mutation newMutation(BiConsumer<Mutation.Builder, NQuad> aggregator, NQuad... nQuads);

    abstract Mutation newMutation(BiConsumer<Mutation.Builder, ByteString> stringNQuadSetter, String nQuads);


    public <T> CompletableFuture<T> query(String query, Map<String, String> variables, TypeToken<T> type) {
        TransactionState state = getState();

        DgraphProto.LinRead linRead = DgraphProto.LinRead.newBuilder()
                .putAllIds(state.getIdsMap())
                .setSequencing(DgraphProto.LinRead.Sequencing.CLIENT_SIDE)
                .build();

        DgraphProto.Request request = DgraphProto.Request.newBuilder()
                .setQuery(query)
                .putAllVars(variables)
                .setStartTs(state.getStartTs())
                .setLinRead(linRead)
                .build();

        StreamObserverBridge<DgraphProto.Response> bridge = new StreamObserverBridge<>();
        getStub().query(request, bridge);

        return bridge.getDelegate()
                .thenApply((DgraphProto.Response response) -> {
                    mergeContext(response.getTxn());

                    String utf8Str = response.getJson().toStringUtf8();
                    if (String.class.isAssignableFrom(type.getRawType())) {
                        return (T) utf8Str;
                    }
                    return PARSER.fromJson(utf8Str, type.getType());
                });
    }


    public <T> CompletableFuture<T> query(String query, TypeToken<T> type) {
        return query(query, emptyMap(), type);
    }

    public <T> CompletableFuture<T> query(String query, Class<T> type) {
        return query(query, emptyMap(), TypeToken.get(type));
    }

    public <T> CompletableFuture<T> query(Supplier<Query> queryFactory, TypeToken<T> type) {
        Query query = queryFactory.get();
        return query(query.getQuery(), query.getVariables(), type);
    }

    public <T> CompletableFuture<T> query(Supplier<Query> queryFactory, Class<T> type) {
        Query query = queryFactory.get();
        return query(query.getQuery(), query.getVariables(), TypeToken.get(type));
    }

    protected CompletableFuture<Map<String, String>> mutate(Mutation mutation) {
        StreamObserverBridge<DgraphProto.Assigned> bridge = new StreamObserverBridge<>();
        DgraphStub stub = getStub();
        stub.mutate(mutation, bridge);
        return bridge.getDelegate()
                .handle((DgraphProto.Assigned assigned, Throwable throwable) -> {
                    if (throwable != null) {
                        // IMPORTANT: the discard is asynchronous meaning that the remote
                        // transaction may or may not be cancelled when this CompletionStage finishes.
                        // All errors occurring during the discard are ignored.
                        abort(stub);
                        throw launderException(throwable);
                    } else {
                        mergeContext(assigned.getContext());
                        return assigned.getUidsMap();
                    }
                });
    }

    protected RuntimeException launderException(Throwable ex) {
        if (ex instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeEx = (StatusRuntimeException) ex;
            Status.Code code = statusRuntimeEx.getStatus().getCode();
            String desc = statusRuntimeEx.getStatus().getDescription();

            if (code.equals(Status.Code.ABORTED)) {
                return new TransactionAbortedException(desc);
            }
        }

        if (ex instanceof RuntimeException) {
            return (RuntimeException) ex;
        }

        return new CompletionException(ex);
    }

    protected CompletableFuture<Void> abort(DgraphStub stub) {
        TransactionState state = getState();

        DgraphProto.LinRead linRead = DgraphProto.LinRead.newBuilder()
                .putAllIds(state.getIdsMap())
                .setSequencing(DgraphProto.LinRead.Sequencing.CLIENT_SIDE)
                .build();

        TxnContext context = TxnContext.newBuilder()
                .setStartTs(state.getStartTs())
                .addAllKeys(state.getKeys())
                .addAllPreds(state.getPreds())
                .setLinRead(linRead)
                .setAborted(true)
                .build();

        StreamObserverBridge<TxnContext> bridge = new StreamObserverBridge<>();
        stub.commitOrAbort(context, bridge);
        return bridge.getDelegate()
                .handle((TxnContext ctx, Throwable throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof StatusRuntimeException) {
                            StatusRuntimeException statusRuntimeEx = (StatusRuntimeException) throwable;

                            //dgraph returns ABORTED status even when user explicitly asks for abort
                            if (!Status.Code.ABORTED.equals(statusRuntimeEx.getStatus().getCode())) {
                                LOG.warn("Exception while aborting transaction", throwable);
                            }
                        } else {
                            LOG.warn("Exception while aborting transaction", throwable);
                        }
                    }
                    return null;
                });
    }

    public CompletableFuture<Map<String, String>> del(String nQuads) {
        return mutate(newMutation(Mutation.Builder::setDelNquads, nQuads));
    }

    public CompletableFuture<Map<String, String>> del(NQuads nQuads) {
        return del(nQuads.toString());
    }

    public CompletableFuture<Map<String, String>> del(Supplier<NQuads> nQuadsSupplier) {
        return del(nQuadsSupplier.get());
    }

    public CompletableFuture<Map<String, String>> set(String nQuads) {
        return mutate(newMutation(Mutation.Builder::setSetNquads, nQuads));
    }

    public CompletableFuture<Map<String, String>> set(NQuads nQuads) {
        return set(nQuads.toString());
    }

    public CompletableFuture<Map<String, String>> set(Supplier<NQuads> nQuadsSupplier) {
        return set(nQuadsSupplier.get());
    }

    public CompletableFuture<Map<String, String>> del(NQuad... nQuads) {
        return mutate(newMutation(Mutation.Builder::addDel, nQuads));
    }

    public CompletableFuture<Map<String, String>> set(NQuad... nQuads) {
        return mutate(newMutation(Mutation.Builder::addSet, nQuads));
    }
}
