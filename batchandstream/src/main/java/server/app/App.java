package main.java.server.app;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import main.java.common.graph.Graph;
import main.java.common.pair.Pair;
import main.java.server.source.Source;
import spark.Spark;

import java.util.List;

public class App {

    private final Source source;
    private final Graph graph;
    private final static Gson gson = new Gson();


    public App(Source source, Graph graph) {
        this.source = source;
        this.graph = graph;
    }

    public void init() {

        Spark.setPort(8888);
        Spark.setIpAddress("localhost");

        Spark.put("/pair/:key/:val", ((request, response) -> {
            String key = request.params("key");
            String value = request.params("val");
            Pair p = new Pair(key, value);
            source.put(p);
            response.status(200);
            return p + " put\n";
        }));

        Spark.post("/pairs", ((request, response) -> {
            String json = request.body();
            List<Pair> pairs = gson.fromJson(json, new TypeToken<List<Pair>>(){}.getType());
            source.put(pairs);
            response.status(200);
            return "Pairs posted\n";
        }));

        Spark.get("/status", ((request, response) -> {
            response.type("application/json");
            response.status(200);
            return graph.toString();
        }));

        Spark.post("/stop", ((request, response) -> {
            source.close();
            response.status(200);
            return "Stream stopped\n";
        }));
    }

    public void close(){
        Spark.stop();
    }
}
