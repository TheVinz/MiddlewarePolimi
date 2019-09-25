package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Node implements Serializable {

    private final int numOperators;
    final List<Node> next = new ArrayList<>();

    Node(int numOperators) {
        if(numOperators < 1)
            throw new IllegalArgumentException("The number of operators must be at least 1");
        this.numOperators = numOperators;
    }

    Node(){
        numOperators = 5;
    }

    public List<Node> getNext() {
        return Collections.unmodifiableList(this.next);
    }

    void setNext(Node next) {
        if(this.next.isEmpty())
            this.next.add(next);
        else
            throw new RuntimeException("Nonempty next list");

        assert this.next.size() == 1;
    }

    public int getNumOperators() {
        return numOperators;
    }

    public List<ActorRef> instantiate(Supervisor supervisor){
        return null;
    }

    abstract JsonObject toJson(JsonGraphBuilder builder);
}
