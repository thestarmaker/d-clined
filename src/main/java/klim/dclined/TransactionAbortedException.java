package klim.dclined;

/**
 * @author Michail Klimenkov
 */
public class TransactionAbortedException extends RuntimeException {
    public TransactionAbortedException(String message) {
        super(message);
    }
}
