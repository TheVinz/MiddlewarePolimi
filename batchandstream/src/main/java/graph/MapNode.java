package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;
import main.java.interfaces.MapFunction;

import java.util.List;

public class MapNode extends Node {

    private Node next;
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

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
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
