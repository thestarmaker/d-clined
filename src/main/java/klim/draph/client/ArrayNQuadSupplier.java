package klim.draph.client;

import io.dgraph.DgraphProto.NQuad;

@FunctionalInterface
public interface ArrayNQuadSupplier {
    NQuad[] get();
}
