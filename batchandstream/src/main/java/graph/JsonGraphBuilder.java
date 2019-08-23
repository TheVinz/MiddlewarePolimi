package main.java.graph;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class JsonGraphBuilder {

    private Gson gson = new Gson();
    private JsonObject merge = null;

    public String build(Graph graph){
        return gson.toJson(build(graph.getSourceNode()));
    }

    JsonObject build(SourceNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Source operator");
        json.addProperty("period", node.getPeriod());
        json.add("next", node.getNext().toJson(this));
        return json;
    }

    JsonObject build(AggregateNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Aggregate operator");
        json.addProperty("instantiated_workers", node.getNumOperators());
        json.addProperty("window_size", node.getWindowSize());
        json.addProperty("window_slide", node.getWindowSlide());
        json.add("next", node.getNext().toJson(this));
        return json;
    }

    JsonObject build(FilterNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Filter operator");
        json.addProperty("instantiated_workers", node.getNumOperators());
        json.add("next", node.getNext().toJson(this));
        return json;
    }

    JsonObject build(FlatMapNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "FlatMap operator");
        json.addProperty("instantiated_workers", node.getNumOperators());
        json.add("next", node.getNext().toJson(this));
        return json;
    }

    JsonObject build(MapNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Map operator");
        json.addProperty("instantiated_workers", node.getNumOperators());
        json.add("next", node.getNext().toJson(this));
        return json;
    }

    JsonObject build(MergeNode node){
        if(merge == null){
            JsonObject json = new JsonObject();
            json.addProperty("type", "Merge operator");
            json.addProperty("instantiated_workers", node.getNumOperators());
            json.add("next", node.getNext().toJson(this));
            merge = json;
        }
        return null;
    }

    JsonObject build(SinkNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Sink operator");
        return json;
    }

    JsonObject build(SplitNode node){
        JsonObject json = new JsonObject();
        json.addProperty("type", "Split operator");
        json.addProperty("instantiated_workers", node.getNumOperators());
        JsonArray next = new JsonArray(node.getNext().size());
        node.getNext().forEach(n -> next.add(n.toJson(this)));
        json.add("next", next);
        json.add("merge", merge);
        merge = null;
        return json;
    }
}
