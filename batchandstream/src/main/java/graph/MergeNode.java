package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;

import java.util.List;

public class MergeNode extends Node {

    private Node next;

    public MergeNode(){}
    public MergeNode(int numOperators){
        super(numOperators);
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateMergeOperator(this);
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
