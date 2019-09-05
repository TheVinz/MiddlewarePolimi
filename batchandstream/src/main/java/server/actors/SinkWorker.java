package main.java.server.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import main.java.server.node.Sink;
import main.java.server.message.EndMessage;
import main.java.common.pair.Pair;

public class SinkWorker extends AbstractActor {

    private class Timer implements Runnable{

        private final long timeout;

        private Timer(long timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(timeout);
                timeout();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private final Sink sink;
    private int endMessagesCount;
    private Thread currentTimer;
    private final static long TIMEOUT = 5000L;


    public SinkWorker(Sink sink, int endMessagesCount) {
        this.sink = sink;
        this.endMessagesCount = endMessagesCount;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Pair.class, sink::putPair)
                .match(EndMessage.class, m -> {
                    currentTimer = new Thread(new Timer(TIMEOUT));
                    currentTimer.start();
                    endMessagesCount--;
                    if(endMessagesCount <=0) {
                        currentTimer = null;
                        getContext().getParent().tell(m, self());
                        System.out.println("Sink received all the end messages!!");
                    }
                })
                .build();
    }

    private synchronized void timeout(){
        if(Thread.currentThread().equals(currentTimer))
            getContext().getParent().tell(new EndMessage(), self());
    }

    public static Props props(Sink sink, int endMessagesCount){
        return Props.create(SinkWorker.class, sink, endMessagesCount);
    }
}
