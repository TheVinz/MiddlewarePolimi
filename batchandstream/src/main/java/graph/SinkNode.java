package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;
import main.java.interfaces.Sink;

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
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateSink(this);
    }
    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
