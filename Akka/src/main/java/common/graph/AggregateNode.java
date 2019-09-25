package main.java.common.graph;

import akka.actor.ActorRef;
import com.google.gson.JsonObject;
import main.java.server.actors.Supervisor;
import main.java.common.interfaces.AggregateFunction;

import java.util.List;

public class AggregateNode extends Node {

    private final AggregateFunction function;
    private final int windowSize;
    private final int windowSlide;

    AggregateNode(AggregateFunction function, int numOperators, int windowSize, int windowSlide){
        super(numOperators);
        if(windowSize < 1 || windowSlide < 1)
            throw new IllegalArgumentException("Window size and window slide must be greater than 0");
        this.function = function;
        this.windowSize=windowSize;
        this.windowSlide = windowSlide;
    }
    AggregateNode(AggregateFunction function, int windowSize, int windowSlide){
        if(windowSize < 1 || windowSlide < 1)
            throw new IllegalArgumentException("Window size and window slide must be greater than 0");
        this.function = function;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }
    AggregateNode(AggregateFunction function) {
        this(function, 8, 3);
    }

    public AggregateFunction getFunction() {
        return function;
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
