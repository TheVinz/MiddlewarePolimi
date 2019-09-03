package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;

import java.util.List;

public class SplitNode extends Node {


    public SplitNode(){
    }
    public SplitNode(int numOperators){
        super(numOperators);
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
