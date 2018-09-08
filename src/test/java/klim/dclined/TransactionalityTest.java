package klim.dclined;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
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

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static klim.dclined.NQuadsFactory.nQuad;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Michail Klimenkov
 */
public class TransactionalityTest {

    protected static DClined client;

    @BeforeAll
    public static void prepare() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9080)
                .usePlaintext()
                .build();
        client = new DClined(channel);
    }

    @BeforeEach
    public void prepareEach() {
        client.dropAll().join();
    }

    @AfterAll
    public static void destroy() throws InterruptedException {
        client.dropAll().join();
        client.close();
    }

    @Test
    public void testVisibility() throws ExecutionException, InterruptedException {
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
    public void testConflict_TransactionOverlapsTransaction() throws ExecutionException, InterruptedException {
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
        lateTrans.commit()
                .handle(this::assertTransactionAbortedExceptionHappened)
                .join();
    }

    @Test
    public void testConflict_TransactionEnclosesTransaction() throws ExecutionException, InterruptedException {
        client.schema("person.username: string @index(hash) . \n" +
                "person.email: string . ").join();

        client.set("_:person <person.username> \"starmaker\" .").join();

        final String getPersonByUsername = "{ starm(func: eq(person.username, \"starmaker\")) { uid person.username person.email} }";

        Transaction outerTrans = client.newTransaction();
        Map<String, List<Map<String, String>>> person = outerTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        final String uid = person.get("starm").get(0).get("uid");

        Transaction innerTrans = client.newTransaction();
        person = innerTrans.query(getPersonByUsername, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        //inner transaction writes and commits
        innerTrans.set(format("<%s> <person.email> \"starmaker@mail.com\" .", uid)).join();
        innerTrans.commit().join();

        //then outer transaction writes and commits
        outerTrans.set(format("<%s> <person.email> \"starmaker@mail.com\" .", uid)).join();
        outerTrans.commit()
                .handle(this::assertTransactionAbortedExceptionHappened)
                .join();
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
    public void testCommitAfterAbort() throws ExecutionException, InterruptedException {
        client.schema("person.email: string @index(hash) .").join();
        Transaction transaction = client.newTransaction();
        transaction.set("_:person <person.email> \"starmaker@mail.com\" .").join();

        final String getPersonByEmail = "{ starm(func: eq(person.email, \"starmaker@mail.com\")) { person.email} }";

        Map<String, List<Map<String, String>>> person = transaction.query(getPersonByEmail, Map.class).get();
        assertFalse(person.get("starm").isEmpty());

        transaction.abort().join();
        transaction.commit()
                .handle(this::assertTransactionAbortedExceptionHappened)
                .join();

        person = transaction.query(getPersonByEmail, Map.class).get();
        assertTrue(person.get("starm").isEmpty());
    }

    @Test
    public void testUpsert() {
        //NOTE: @upsert directive is essential for index conflict detection
        client.schema("person.email: string @index(hash) @upsert .").join();

        final String getPersonByEmail = "{ starm(func: eq(person.email, \"starmaker@mail.com\")) { person.email} }";

        TypeToken<Map<String, List<Person>>> responseType = new TypeToken<Map<String, List<Person>>>() {};

        Transaction transaction = client.newTransaction();
        transaction.query(getPersonByEmail, responseType)
                .thenCompose((Map<String, List<Person>> response) -> {
                    //if the person with the email does not exist, create one
                    if (response.getOrDefault("starm", emptyList()).isEmpty()) {

                        //pretend there is some other chicky write happening in the meantime
                        client.set("_:person <person.email> \"starmaker@mail.com\" .").join();

                        //define the mutations using nQuad syntax
                        NQuads nQuads = nQuad("_:person", "person.email", "starmaker@mail.com");
                        //create the new person as assumed by the IF statement
                        return transaction.set(nQuads);
                    }
                    throw new RuntimeException("User already exists");
                }).thenCompose((assigned) -> {
            return transaction.commit();
        }).handle(this::assertTransactionAbortedExceptionHappened)
                .join();

    }

    public static class Person {
        @SerializedName("person.email")
        private final String email;

        public Person(String email) {
            this.email = email;
        }

        public String getEmail() {
            return email;
        }
    }

    private Void assertTransactionAbortedExceptionHappened(Void aVoid, Throwable throwable) {
        assertThrows(CompletionException.class, () -> {
            throw throwable;
        });
        assertThrows(TransactionAbortedException.class, () -> {
            throw throwable.getCause();
        });
        return null;
    }
}
