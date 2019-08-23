package main.java.graph;

public class Graph {
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
