package main.java.server.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.common.interfaces.FilterFunction;
import main.java.common.pair.Pair;

import java.util.List;

public class FilterWorker extends AbstractWorker {

    private final FilterFunction function;

    public FilterWorker(FilterFunction function, List<ActorRef> downstreamOperators){
        super(downstreamOperators);
        this.function = function;
    }

    void onPair(Pair pair){
        setCurrent(pair);
        if(function.filter(pair))
            getDownstramOperators().get(pair.getKey().hashCode() % getDownstramOperators().size()).tell(pair, self());
        setCurrent(null);
    }

    public static Props props(FilterFunction function, List<ActorRef> downstreamOperators){
        return Props.create(FilterWorker.class, function, downstreamOperators);
    }
}
