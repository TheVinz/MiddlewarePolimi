package main.java.common.graph;

import java.io.Serializable;

public class Graph implements Serializable {
    private final SourceNode sourceNode;

    public Graph(SourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    public SourceNode getSourceNode() {
        return sourceNode;
    }

    @Override
    public String toString(){
        return new JsonGraphBuilder().build(this);
    }

}
