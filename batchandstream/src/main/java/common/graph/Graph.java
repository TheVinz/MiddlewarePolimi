package main.java.common.graph;

import com.google.gson.JsonObject;

import java.io.Serializable;

public class Graph implements Serializable {
    private final SourceNode sourceNode;

    public Graph(SourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public SourceNode getSourceNode() {
        return sourceNode;
    }

    public JsonObject toJson(){
        return new JsonGraphBuilder().buildJson(this);
    }

    @Override
    public String toString(){
        return new JsonGraphBuilder().build(this);
    }

}
