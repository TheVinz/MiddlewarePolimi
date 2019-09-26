package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;

import java.util.List;

public class SplitNode extends Node {

    private boolean merged = false;

    SplitNode(){}
    SplitNode(int numOperators){
        super(numOperators);
    }

    void merge(){
        merged = true;
    }

    @Override
    void setNext(Node next){
        if(merged)
            throw new RuntimeException("Already merged split");
        this.next.add(next);
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
