package klim.draph.client;

import java.util.Map;

import static java.util.Collections.emptyMap;

public class Query {
    private final String query;
    private final Map<String, String> variables;

    public Query(String query) {
        this(query, emptyMap());
    }

    public Query(String query, Map<String, String> variables) {
        this.query = query;
        this.variables = variables;
    }

    public String getQuery() {
        return query;
    }

    public Map<String, String> getVariables() {
        return variables;
    }
}
