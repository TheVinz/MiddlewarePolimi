package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;
import main.java.common.interfaces.Sink;

import java.util.List;

public class SinkNode extends Node {

    private final Sink sink;

    public SinkNode(Sink sink) {
        super(1);
        this.sink = sink;
    }

    public Sink getSink() {
        return sink;
    }

    @Override
    public Node getNext() {
        return null;
    }

    @Override
    public void setNext(Node next) {
        throw new RuntimeException("Adding next to Sink");
    }

    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateSink(this);
    }
    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
