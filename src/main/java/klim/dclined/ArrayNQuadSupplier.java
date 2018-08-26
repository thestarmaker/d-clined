package klim.dclined;

import io.dgraph.DgraphProto.NQuad;

/**
 * @author Michail Klimenkov
 */
@FunctionalInterface
public interface ArrayNQuadSupplier {
    NQuad[] get();
}
