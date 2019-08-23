package main.java.actors;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import main.java.graph.*;
import main.java.message.EndMessage;
import main.java.graph.Graph;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Supervisor extends AbstractActor {

    private List<ActorRef> merge = null;
    private int layer = 0;
    static final int MAX_RETRIES = 10;
    int lastLayerSize = 0;

    private List<ActorRef> instantiateOperator(Node node){
        return node.instantiate(this);
    }

    public List<ActorRef> instantiateFilterOperator(FilterNode node) {
        int layer = this.layer ++;
        lastLayerSize = node.getNumOperators();
        List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
        List<ActorRef> result = new ArrayList<>();
        for(int i=0; i<node.getNumOperators(); i++){
            ActorRef actorRef = getContext()
                    .actorOf(FilterWorker.props(node.getFunction(), downstreamOperator), "Filter_" + layer + "_" + i );
            getContext().watch(actorRef);
            result.add(actorRef);
        }
        return result;
    }

    public List<ActorRef> instantiateFlatMapOperator(FlatMapNode node){
        int layer = this.layer ++;
        lastLayerSize = node.getNumOperators();
        List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
        List<ActorRef> result = new ArrayList<>();
        for(int i=0; i<node.getNumOperators(); i++){
            ActorRef actorRef = getContext()
                    .actorOf(FlatMapWorker.props(node.getFunction(), downstreamOperator), "FlatMap_" + layer + "_" + i );
            getContext().watch(actorRef);
            result.add(actorRef);
        }
        return result;
    }

    public List<ActorRef> instantiateMapWorker(MapNode node){
        int layer = this.layer ++;
        lastLayerSize = node.getNumOperators();
        List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
        List<ActorRef> result = new ArrayList<>();
        for(int i=0; i<node.getNumOperators(); i++){
            ActorRef actorRef = getContext()
                    .actorOf(MapWorker.props(node.getFunction(), downstreamOperator), "Map_" + layer + "_" + i );
            getContext().watch(actorRef);
            result.add(actorRef);
        }
        return result;
    }

    public List<ActorRef> instantiateMergeOperator(MergeNode node) {
        int layer = this.layer ++;
        List<ActorRef> result = new ArrayList<>();
        if(merge == null) {
            lastLayerSize = node.getNumOperators();
            List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
            for (int i = 0; i < node.getNumOperators(); i++) {
                ActorRef actorRef = getContext()
                        .actorOf(MergeWorker.props(downstreamOperator), "Merge_" + layer + "_" + i );
                getContext().watch(actorRef);
                result.add(actorRef);
            }
            merge = result;
        } else {
            result = merge;
        }
        return result;
    }

    public List<ActorRef> instantiateSink(SinkNode node){
        layer ++;
        ActorRef actorRef =  getContext().actorOf(
                SinkWorker.props(node.getSink(), lastLayerSize), "Sink");
        getContext().watch(actorRef);
        return Collections.singletonList(actorRef);
    }

    public List<ActorRef> instantiateSplitOperator(SplitNode node){
        int layer = this.layer ++;
        List<List<ActorRef>> downstreamOperators = new ArrayList<>();
        lastLayerSize = node.getNumOperators();
        for(Node n : node.getNext())
            downstreamOperators.add(instantiateOperator(n));
        merge = null;
        List<ActorRef> result = new ArrayList<>();
        for(int i=0; i<node.getNumOperators(); i++){
            ActorRef actorRef = getContext()
                    .actorOf(SplitWorker.props(downstreamOperators), "Split_" + layer + "_" + i );
            getContext().watch(actorRef);
            result.add(actorRef);
        }
        return result;
    }

    public List<ActorRef> instantiateAggregateOperator(AggregateNode node){
        int layer = this.layer ++;
        lastLayerSize = node.getNumOperators();
        List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
        List<ActorRef> result = new ArrayList<>();
        for(int i=0; i<node.getNumOperators(); i++){
            ActorRef actorRef = getContext()
                    .actorOf(AggregateWorker.props(
                            node.getFunction(), node.getWindowSize(), node.getWindowSlide(), downstreamOperator),
                            "Aggregate_" + layer + "_" + i );
            getContext().watch(actorRef);
            result.add(actorRef);
        }
        return result;
    }

    private void instantiate(SourceNode node){
        List<ActorRef> downstreamOperator = instantiateOperator(node.getNext());
        getContext().actorOf(SourceActor.props(node.getSource(), node.getPeriod(), downstreamOperator), "Source");
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(
                2 * MAX_RETRIES,
                Duration.create("1 seconds"), //
                DeciderBuilder //
                        .match(Exception.class, ex -> SupervisorStrategy.restart())
                        .build());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Graph.class, g -> instantiate(g.getSourceNode()))
                .match(EndMessage.class, m -> {
                    getContext().stop(self());
                })
                .match(Terminated.class, m -> {
                    System.err.println(m.getActor() + " terminated unexpectedly, shutting down the stream.");
                    getContext().stop(self());
                })
                .build();
    }

    public static Props props(){
        return Props.create(Supervisor.class);
    }
}
