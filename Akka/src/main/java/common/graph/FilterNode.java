package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;
import main.java.common.interfaces.FilterFunction;

import java.util.List;

public class FilterNode extends Node {

    private final FilterFunction function;

    FilterNode(FilterFunction function, int numOperators){
        super(numOperators);
        this.function = function;
    }
    FilterNode(FilterFunction function) {
        this.function = function;
    }

    public FilterFunction getFunction() {
        return function;
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
