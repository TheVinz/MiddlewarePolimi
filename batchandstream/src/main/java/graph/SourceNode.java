package main.java.graph;

import com.google.gson.JsonObject;
import main.java.interfaces.Source;

public class SourceNode extends Node {

    private final Source source;
    private final long period;
    private Node next;

    public SourceNode(Source source, long period) {
        super(1);
        this.source = source;
        this.period=period;
    }

    public long getPeriod() {
        return period;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
    }

    public Source getSource() {
        return source;
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
