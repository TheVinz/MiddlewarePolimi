package main.java.common.graph;

import com.google.gson.JsonObject;

public class SourceNode extends Node {

    private final long period;

    SourceNode( long period) {
        super(1);
        this.period=period;
    }

    public long getPeriod() {
        return period;
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
