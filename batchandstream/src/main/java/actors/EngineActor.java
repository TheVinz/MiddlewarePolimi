package main.java.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import main.java.graph.Graph;

public class EngineActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Graph.class, g -> {
                    ActorRef child = getContext().actorOf(Supervisor.props(), "Supervisor");
                    child.tell(g, self());
                })
                .build();
    }

    public static Props props(){
        return Props.create(EngineActor.class);
    }
}
