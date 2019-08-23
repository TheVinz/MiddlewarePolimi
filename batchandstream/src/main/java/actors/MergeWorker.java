package main.java.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.message.Pair;

import java.util.List;

public class MergeWorker extends AbstractWorker {


    public MergeWorker(List<ActorRef> downstreamOperators) {
        super(downstreamOperators);
    }

     void onPair(Pair pair){
        setCurrent(pair);
        getDownstramOperators().get(pair.getKey().hashCode() % getDownstramOperators().size()).tell(pair, self());
        setCurrent(null);
    }

    public static Props props(List<ActorRef> downstreamOperators){
        return Props.create(MergeWorker.class, downstreamOperators);
    }
}
