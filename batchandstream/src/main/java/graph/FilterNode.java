package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;
import main.java.interfaces.FilterFunction;

import java.util.List;

public class FilterNode extends Node {

    private Node next;
    private final FilterFunction function;

    public FilterNode(FilterFunction function, int numOperators){
        super(numOperators);
        this.function = function;
    }
    public FilterNode(FilterFunction function) {
        this.function = function;
    }

    public FilterFunction getFunction() {
        return function;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
    }


    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateFilterOperator(this);
    }
    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
