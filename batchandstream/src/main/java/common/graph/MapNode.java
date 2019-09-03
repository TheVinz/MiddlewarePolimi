package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;
import main.java.common.interfaces.MapFunction;

import java.util.List;

public class MapNode extends Node {

    private final MapFunction function;

    public MapNode(MapFunction function, int numOperators){
        super(numOperators);
        this.function=function;
    }
    public MapNode(MapFunction function){
        this.function=function;
    }

    public MapFunction getFunction() {
        return function;
    }

    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateMapWorker(this);
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
