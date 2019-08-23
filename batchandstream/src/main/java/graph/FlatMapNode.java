package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;
import main.java.interfaces.FlatMapFunction;

import java.util.List;

public class FlatMapNode extends Node {

    private Node next;
    private final FlatMapFunction function;

    public FlatMapNode(FlatMapFunction function) {
        this.function = function;
    }
    public FlatMapNode(FlatMapFunction function, int numOperators) {
        super(numOperators);
        this.function = function;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
    }

    public FlatMapFunction getFunction() {
        return function;
    }

    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateFlatMapOperator(this);
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
