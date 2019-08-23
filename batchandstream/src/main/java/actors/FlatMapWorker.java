package main.java.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.interfaces.FlatMapFunction;
import main.java.message.Pair;

import java.util.List;

public class FlatMapWorker extends AbstractWorker {

    private final FlatMapFunction function;

    public FlatMapWorker(FlatMapFunction function, List<ActorRef> downstreamOperators) {
        super(downstreamOperators);
        this.function = function;
    }

    void onPair(Pair pair){
        setCurrent(pair);
        List<Pair> res = function.flatMap(pair);

        res.forEach(p -> getDownstramOperators().get(p.getKey().hashCode() % getDownstramOperators().size()).tell(p, self()));

        setCurrent(null);
    }

    public static Props props(FlatMapFunction function, List<ActorRef> downstreamOperators){
        return Props.create(FlatMapWorker.class, function, downstreamOperators);
    }
}
