# D-clined: Alternative DGraph Java client #
## Motivation ##
This is ~~sane~~ an alternative Java implementation of [DGraph client](https://github.com/dgraph-io/dgraph4j) aiming to provide cleaner and more fluid API as well as to get around lengthy pull request approval process. 
Neither this project nor the author bear any affiliation with DGraph.

I will roll it the way I like, take it or leave it! :stuck_out_tongue_winking_eye:
## Usage ## 
### How do I get it to my project? ###
I have not published it to Maven Central yet, so you have to build it yourself.

### How do I build it locally? ###
__IMPORTANT - tests assume you have local instance of DGraph running on localhost:9080 and will wipe out all data in database!__
Project is assembled in Maven (why not?), so run:
```
mvn clean install
```
This puts the artifact to your local repository, then just bring it to your project:
```
<dependency>
    <groupId>klim.dclined</groupId>
    <artifactId>d-clined</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### How do I configure the client? ###
```
ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9080)
                                     .usePlaintext()    //skip this line if you are using TLS
                                     .build();
DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
DGraphClient client = new DGraphClient(stub);
```

### How do I do queries/mutations? ###
 The provided API is completely asynchronous and implies two major use cases:
 * One off operations
 * Transactional operations
 
 For one-off operations use DGraphClient instances directly:
 ```
client.query("{ starm(func: eq(person.email, \"starmaker@mail.com\")) { person.email } }", Map.class)
            .thenAccept((Map response) -> {
                //handling goes here
            });
 ```
 
 For transactional operations create a transaction and use that instead. Consider situation when all users must have unique e-mail addresses. 
 Define Schema:
 ```
//NOTE: @upsert directive is essential for index conflict detection
client.schema("person.email: string @index(hash) @upsert .").join();
 ```
 Either define some DTO or use pure collections:
 ```
public class Person {
    @SerializedName("person.email")
    private final String email;

    public Person(String email) {
        this.email = email;
    }

    public String getEmail() {
         return email;
    }
}
 ```
The transactional check-n-write would look like this:
```
Class<Map<String, List<Person>>> responseType = (Class<Map<String, List<Person>>>) new TypeToken<Map<String, List<Person>>>(){}.getRawType();

Transaction transaction = client.newTransaction();
transaction.query(getPersonByEmail, responseType)
        .thenCompose((Map<String, List<Person>> response) -> {
            //if the person with the email does not exist, create one
            if (response.getOrDefault("starm", emptyList()).isEmpty()) {
                //create the new person as assumed by the IF statement
                return transaction.set("_:person <person.email> \"starmaker@mail.com\" .");
            }
            throw new RuntimeException("User already exists");
        }).thenCompose((assigned) -> {
            return transaction.commit();
        }).handle(<your handling goes here>);
```
If at any time during the transaction any other transaction manages to sneak in a person with the same email address, then the given transaction would abort with TransactionAbortedException.

### How do I deserialise responses to custom types? ###
D-clined uses [Gson](https://github.com/google/gson/) for deserialisation; all Gson annotations should also be supported.

### How do I configure D-clined to connect to multiple DGraph servers? ###
You don't. This is not supported as yet because there is more to this: 
* What happens if one of the configured servers goes offline?
* What happens if it never returns?

D-clined assumes this is your responsibility to provide load balancing across multiple DGraph instances, hence D-clined can be configured with only one network address - one that probably should direct to load balancer.
 