package main.java.server.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.server.message.EndMessage;
import main.java.common.pair.Pair;

import java.util.List;

public class SplitWorker extends AbstractWorker {

    private final List<List<ActorRef>> downstreamOperators;

    public SplitWorker( List<List<ActorRef>> downstreamOperators) {
        this.downstreamOperators = downstreamOperators;
    }

    void onPair(Pair pair){
        setCurrent(pair);
        downstreamOperators.forEach(list -> list.get(pair.getKey().hashCode() % list.size()).tell(pair, self()));
        setCurrent(null);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Pair.class, this::onPair)
                .match(EndMessage.class, m -> {
                    if(!isClosed()){
                        for(List<ActorRef> list : downstreamOperators)
                            list.forEach(a -> a.tell(m, self()));
                        close();
                    }
                })
                .build();
    }

    public static Props props(List<List<ActorRef>> downstreamOperators){
        return Props.create(SplitWorker.class, downstreamOperators);
    }
}
