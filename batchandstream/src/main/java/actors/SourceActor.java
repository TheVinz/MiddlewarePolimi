package main.java.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.interfaces.Source;
import main.java.message.EndMessage;
import main.java.message.Pair;

import java.util.List;

public class SourceActor extends AbstractActor implements Runnable{

    private final Source source;
    private final long period;
    private final List<ActorRef> downstreamOperator;
    private boolean stop = false;

    SourceActor(Source source, long period, List<ActorRef> downstreamOperator) {
        this.source = source;
        this.period = period;
        this.downstreamOperator = downstreamOperator;
        new Thread(this).start();
    }

    @Override
    public void run() {
        Pair p = source.getPair();
        try {
            while (p!= null && !stop) {
                if(downstreamOperator.size() != 0)
                    downstreamOperator.get(p.getKey().hashCode() % downstreamOperator.size()).tell(p, self());
                Thread.sleep(period);
                p = source.getPair();
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        downstreamOperator.forEach(a -> a.tell(new EndMessage(), self()));
        System.out.println("Source terminated!!");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .build();
    }

    @Override
    public void postStop() {
        this.stop = true;
    }

    public static Props props(Source source, long period, List<ActorRef> downstreamOperator){
        return Props.create(SourceActor.class, source, period, downstreamOperator);
    }
}