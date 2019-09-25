package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;
import main.java.common.interfaces.FlatMapFunction;

import java.util.List;

public class FlatMapNode extends Node {

    private final FlatMapFunction function;

    FlatMapNode(FlatMapFunction function) {
        this.function = function;
    }
    FlatMapNode(FlatMapFunction function, int numOperators) {
        super(numOperators);
        this.function = function;
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
