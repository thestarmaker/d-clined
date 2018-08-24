package klim.draph.client;

import com.google.gson.JsonObject;
import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        JsonObject person = transaction.query(getPersonByEmail).get();
        assertEquals(personResponse, person.toString());

        //but not visible for client till commit
        person = client.query(getPersonByEmail).get();
        assertEquals(emptyResponse, person.toString());

        //neither is visible for other transactions
        person = client.newTransaction().query(getPersonByEmail).get();
        assertEquals(emptyResponse, person.toString());

        //still visible for the original transaction
        person = transaction.query(getPersonByEmail).get();
        assertEquals(personResponse, person.toString());

        transaction.commit().join();

        //now visible for client
        person = client.query(getPersonByEmail).get();
        assertEquals(personResponse, person.toString());

        //and visible for other transactions
        person = client.newTransaction().query(getPersonByEmail).get();
        assertEquals(personResponse, person.toString());
    }

    @Test
    public void testTransactionConflictsTransaction() throws ExecutionException, InterruptedException {
        client.schema("person.username: string @index(hash) . \n" +
                "person.email: string . ").join();

        client.set("_:person <person.username> \"starmaker\" .").join();

        final String getPersonByUsername = "{ starm(func: eq(person.username, \"starmaker\")) { uid person.username person.email} }";

        Transaction earlyTrans = client.newTransaction();
        JsonObject person = earlyTrans.query(getPersonByUsername).get();
        assertEquals(1, person.getAsJsonArray("starm").size());

        final String uid = person.getAsJsonArray("starm")
                .get(0)
                .getAsJsonObject()
                .getAsJsonPrimitive("uid")
                .getAsString();

        Transaction lateTrans = client.newTransaction();
        person = lateTrans.query(getPersonByUsername).get();
        assertEquals(1, person.getAsJsonArray("starm").size());

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
        JsonObject person = earlyTrans.query(getPersonByUsername).get();
        assertEquals(1, person.getAsJsonArray("starm").size());

        final String uid = person.getAsJsonArray("starm")
                .get(0)
                .getAsJsonObject()
                .getAsJsonPrimitive("uid")
                .getAsString();

        Transaction lateTrans = client.newTransaction();
        person = lateTrans.query(getPersonByUsername).get();
        assertEquals(1, person.getAsJsonArray("starm").size());

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
