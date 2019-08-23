package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;

import java.io.Serializable;
import java.util.List;

public abstract class Node implements Serializable {

    private final int numOperators;

    protected Node(int numOperators) {
        this.numOperators = numOperators;
    }

    Node(){
        numOperators = 5;
    }

    public int getNumOperators() {
        return numOperators;
    }

    public List<ActorRef> instantiate(Supervisor supervisor){
        return null;
    }

    abstract JsonObject toJson(JsonGraphBuilder builder);
}
