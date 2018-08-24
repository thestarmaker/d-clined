package klim.draph.client;

public class TransactionAbortedException extends RuntimeException {
    public TransactionAbortedException(String message) {
        super(message);
    }
}
