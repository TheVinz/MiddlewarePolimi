package main.java.server.actors;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import main.java.common.graph.*;
import main.java.server.app.App;
import main.java.server.message.EndMessage;
import main.java.common.graph.Graph;
import main.java.server.node.Sink;
import main.java.server.node.Source;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Supervisor extends AbstractActor {

    private List<ActorRef> merge = null;
    private int layer = 0;
    static final int MAX_RETRIES = 10;
    private int lastLayerSize = 0;
    private Source source;
    private App app;
    private Sink sink = null;

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
                SinkWorker.props(sink, lastLayerSize), "Sink");
        getContext().watch(actorRef);
        return Collections.singletonList(actorRef);
    }

    public List<ActorRef> instantiateSplitOperator(SplitNode node){
        int layer = this.layer ++;
        List<List<ActorRef>> downstreamOperators = new ArrayList<>();
        lastLayerSize = node.getNumOperators();
        downstreamOperators.add(instantiateOperator(node.getNext()));
        downstreamOperators.add(instantiateOperator(node.getNext()));
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
        source = new Source();
        getContext().actorOf(SourceActor.props(source, node.getPeriod(), downstreamOperator), "Source");
    }

    @Override
    public void postStop() {
        if(app!=null)
            app.close();

        System.out.println("Stopping supervisor...");
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
                .match(Graph.class, g -> {
                    sender().tell("OK", self());
                    if(app == null) {
                        this.sink = new Sink();
                        instantiate(g.getSourceNode());
                        app = new App(source, sink, g);
                        sink = null;
                        app.init();
                    }
                })
                .match(EndMessage.class, m -> app != null, m -> {
                    app.close();
                    app = null;
                    getContext().getChildren().forEach(c -> getContext().stop(c));
                })
                .match(Terminated.class,  m -> {
                    if(app != null) {
                        System.err.println(m.getActor() + " terminated unexpectedly, shutting down the stream.");
                        app.close();
                        app = null;
                        getContext().getChildren().forEach(c -> getContext().stop(c));
                    }
                })
                .build();
    }

    public static Props props(){
        return Props.create(Supervisor.class);
    }
}
