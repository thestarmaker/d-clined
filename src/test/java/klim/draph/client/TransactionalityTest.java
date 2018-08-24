package klim.draph.client;

import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

public class TransactionalityTest {

    private static ManagedChannel channel;
    private static DGraphClient client;

    @BeforeAll
    public static void prepare() {
        channel = ManagedChannelBuilder.forAddress("localhost", 9080)
                .usePlaintext()
                .build();
        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
        client = new DGraphClient(stub);
    }

    @BeforeEach
    public void prepareEach() {
        client.dropAll().join();
    }

    @AfterAll
    public static void destroy() throws InterruptedException {
        channel.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testTransactionality() throws ExecutionException, InterruptedException {
        client.schema("person.email: string @index(hash) .").join();

        Transaction transaction = client.newTransaction();
        transaction.set("_:person <person.email> \"starmaker@mail.com\" .").join();

        final String getPersonByEmail = "{ starm(func: eq(person.email, \"starmaker@mail.com\")) { person.email } }";
        final String personResponse = "{\"starm\":[{\"person.email\":\"starmaker@mail.com\"}]}";
        final String emptyResponse = "{\"starm\":[]}";

        //visible for the transaction
        String person = transaction.query(getPersonByEmail, String.class).get();
        assertEquals(personResponse, person);

        //but not visible for client till commit
        person = client.query(getPersonByEmail, String.class).get();
        assertEquals(emptyResponse, person);

        //neither is visible for other transactions
        person = client.newTransaction().query(getPersonByEmail, String.class).get();
        assertEquals(emptyResponse, person);

        //still visible for the original transaction
        person = transaction.query(getPersonByEmail, String.class).get();
        assertEquals(personResponse, person);

        transaction.commit().join();

        //now visible for client
        person = client.query(getPersonByEmail, String.class).get();
        assertEquals(personResponse, person);

        //and visible for other transactions
        person = client.newTransaction().query(getPersonByEmail, String.class).get();
        assertEquals(personResponse, person);
    }

    @Test
    public void testTransactionConflictsTransaction() throws ExecutionException, InterruptedException {
        client.schema("person.username: string @index(hash) . \n" +
                "person.email: string . ").join();

        client.set("_:person <person.username> \"starmaker\" .").join();

        final String getPersonByUsername = "{ starm(func: eq(person.username, \"starmaker\")) { uid person.username person.email} }";

        Transaction earlyTrans = client.newTransaction();
        Map<String, List<Map<String, String>>> person = earlyTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        final String uid = person.get("starm").get(0).get("uid");

        Transaction lateTrans = client.newTransaction();
        person = lateTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        //early transaction writes
        earlyTrans.set(format("<%s> <person.email> \"starmaker@mail.com\" .", uid)).join();
        //then late transaction writes different data for the same uid
        lateTrans.set(format("<%s> <person.email> \"starmaker@mail.com\" .", uid)).join();
        //then early transaction commits
        earlyTrans.commit().join();
        //then
        lateTrans.commit().handle((Void aVoid, Throwable throwable) -> {
            assertThrows(CompletionException.class, () -> {
                throw throwable;
            });
            assertThrows(TransactionAbortedException.class, () -> {
                throw throwable.getCause();
            });
            return null;
        }).join();
    }

    @Test
    public void testTransactionWriteInParallel() throws ExecutionException, InterruptedException {
        client.schema("person.username: string @index(hash) . \n" +
                "person.email: string . \n" +
                "person.planet: string .").join();

        client.set("_:person <person.username> \"starmaker\" .").join();

        final String getPersonByUsername = "{ starm(func: eq(person.username, \"starmaker\")) { uid person.username person.email} }";

        Transaction earlyTrans = client.newTransaction();
        Map<String, List<Map<String, String>>> person = earlyTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        final String uid = person.get("starm").get(0).get("uid");

        Transaction lateTrans = client.newTransaction();
        person = lateTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        //early transaction writes email
        earlyTrans.set(format("<%s> <person.email> \"starmaker@mail.com\" .", uid)).join();
        //then late transaction writes planet
        lateTrans.set(format("<%s> <person.planet> \"Earth\" .", uid)).join();
        //then early transaction commits
        earlyTrans.commit().join();
        //then late transaction commits
        lateTrans.commit().join();
    }

    @Test
    public void testCommitAfterAbort() {
        client.schema("person.email: string @index(hash) .").join();
        Transaction transaction = client.newTransaction();
        transaction.set("_:person <person.email> \"starmaker@mail.com\" .").join();
        transaction.abort().join();
        transaction.commit()
                .handle((Void aVoid, Throwable throwable) -> {
                    assertThrows(CompletionException.class, () -> {
                        throw throwable;
                    });
                    assertThrows(TransactionAbortedException.class, () -> {
                        throw throwable.getCause();
                    });
                    return null;
                }).join();
    }
}
