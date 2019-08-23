package main.java.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.actors.Supervisor;
import main.java.interfaces.AggregateFunction;

import java.util.List;

public class AggregateNode extends Node {

    private Node next;
    private final AggregateFunction function;
    private final int windowSize;
    private final int windowSlide;

    public AggregateNode(AggregateFunction function, int numOperators, int windowSize, int windowSlide){
        super(numOperators);
        this.function = function;
        this.windowSize=windowSize;
        this.windowSlide = windowSlide;
    }
    public AggregateNode(AggregateFunction function, int windowSize, int windowSlide){
        this.function = function;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }
    public AggregateNode(AggregateFunction function) {
        this.windowSlide = 3;
        this.windowSize = 8;
        this.function = function;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public int getWindowSlide() {
        return windowSlide;
    }


    @Override
    public List<ActorRef> instantiate(Supervisor supervisor) {
        return supervisor.instantiateAggregateOperator(this);
    }

    @Override
    JsonObject toJson(JsonGraphBuilder builder) {
        return builder.build(this);
    }
}
