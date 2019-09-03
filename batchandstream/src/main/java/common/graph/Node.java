package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;

import java.io.Serializable;
import java.util.List;

public abstract class Node implements Serializable {

    private final int numOperators;
    private Node next;

    protected Node(int numOperators) {
        this.numOperators = numOperators;
    }

    Node(){
        numOperators = 5;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public int getNumOperators() {
        return numOperators;
    }

    public List<ActorRef> instantiate(Supervisor supervisor){
        return null;
    }

    abstract JsonObject toJson(JsonGraphBuilder builder);
}
