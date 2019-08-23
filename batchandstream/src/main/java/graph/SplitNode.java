package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;

import java.util.ArrayList;
import java.util.List;

public class SplitNode extends Node {

    private List<Node> next = new ArrayList<>();

    public SplitNode(){
    }
    public SplitNode(int numOperators){
        super(numOperators);
    }

    public List<Node> getNext() {
        return next;
    }

    public void addNext(Node node){
        next.add(node);
    }

    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateSplitOperator(this);
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
