package main.java.server.app;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import main.java.common.graph.Graph;
import main.java.common.pair.Pair;
import main.java.server.node.Sink;
import main.java.server.node.Source;
import spark.Spark;

import java.io.*;
import java.util.List;

public class App {

    private final Source source;
    private final Graph graph;
    private final static Gson gson = new Gson();
    private final Sink sink;


    public App(Source source, Sink sink, Graph graph) {
        this.source = source;
        this.graph = graph;
        this.sink = sink;
    }

    public void init() {
        Server server;
        try(BufferedReader reader = new BufferedReader(new FileReader(new File("conf/commonconf/server.json")))){
            String line = reader.readLine();
            server = gson.fromJson(line, Server.class);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        Spark.setPort(8080);
        Spark.setIpAddress(server.ip);

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

        Spark.get("status", ((request, response) -> {
            response.type("application/json");
            response.status(200);
            return gson.toJson(getStatus(true));
        }));

        Spark.get("status/no_output", ((request, response) -> {
            response.type("application/json");
            response.status(200);
            return gson.toJson(getStatus(false));
        }));

        Spark.post("/stop", ((request, response) -> {
            source.close();
            response.status(200);
            return "Stream stopped\n";
        }));
    }

    public void close(){
        sink.close();
        Spark.stop();
    }

    private JsonObject getStatus(boolean isOutput){
        JsonObject res = new JsonObject();
        JsonElement graph = this.graph.toJson();
        res.addProperty("Elements still to be processed", source.getQueueSize());
        res.add("Current instantiated graph", graph);
        if(isOutput) {
            JsonElement output = sink.getCurrentOutput();
            res.add("Output", output);
        }
        return res;
    }
}

class Server {
    int port;
    String ip;
}
