package main.java.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.interfaces.MapFunction;
import main.java.message.Pair;

import java.util.List;

public class MapWorker extends AbstractWorker {

    private final MapFunction function;

    public MapWorker(MapFunction function, List<ActorRef> downstreamOperators){
        super(downstreamOperators);
        this.function = function;
    }

    @Override
    void onPair(Pair pair){
        setCurrent(pair);
        Pair res = function.map(pair);
        getDownstramOperators().get(res.getKey().hashCode() % getDownstramOperators().size()).tell(res, self());
        setCurrent(null);
    }

    public static Props props(MapFunction function, List<ActorRef> downstreamOperators){
        return Props.create(MapWorker.class, function, downstreamOperators);
    }
}
