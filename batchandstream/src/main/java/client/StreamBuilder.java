package main.java.client;

import akka.actor.*;
import com.typesafe.config.ConfigFactory;
import main.java.common.graph.*;
import main.java.common.interfaces.*;
import com.typesafe.config.Config;


import java.io.File;

public class StreamBuilder {

    private final Graph graph;
    private boolean ok = false;

    public StreamBuilder(long period){
        SourceNode node = new SourceNode(period);
        this.graph = new Graph(node);
    }

    private void addNode(Node node){
        Node n = graph.getSourceNode();
        while(n.getNext() != null) {
            n = n.getNext();
        }
        n.setNext(node);
    }

    private void addNode(MergeNode node){
        boolean flag = false;
        Node n = graph.getSourceNode();
        while(n.getNext() != null) {
            if(n instanceof SplitNode)
                flag = true;
            else if (n instanceof MergeNode)
                flag = false;
            n = n.getNext();
        }
        if(flag)
            n.setNext(node);
        else
            throw new IllegalStateException("Merge without a Split");
    }

    private void addNode(SinkNode node) {
        boolean flag = true;
        Node n = graph.getSourceNode();
        while(n.getNext() != null) {
            if(n instanceof SplitNode)
                flag = false;
            else if (n instanceof MergeNode)
                flag = true;
            n = n.getNext();
        }
        if(flag)
            n.setNext(node);
        else
            throw new IllegalStateException("Unmerged streams");
    }

    public StreamBuilder aggregate(AggregateFunction function, int numOperators, int windowSize, int windowSlide){
        AggregateNode node  = new AggregateNode(function, numOperators, windowSize, windowSlide);
        addNode(node);
        return this;
    }

    public StreamBuilder aggregate(AggregateFunction func, int windowSize, int windowSlide){
        AggregateNode node  = new AggregateNode(func, windowSize, windowSlide);
        addNode(node);
        return this;
    }

    public StreamBuilder aggregate(AggregateFunction func){
        AggregateNode node  = new AggregateNode(func);
        addNode(node);
        return this;
    }

    public StreamBuilder filter(FilterFunction function) {
        FilterNode node = new FilterNode(function);
        addNode(node
        );
        return this;
    }

    public StreamBuilder filter(FilterFunction function, int numOperators) {
        FilterNode node = new FilterNode(function, numOperators);
        addNode(node
        );
        return this;
    }

    public StreamBuilder flatMap(FlatMapFunction function) {
        FlatMapNode node = new FlatMapNode(function);
        addNode(node
        );
        return this;
    }

    public StreamBuilder flatMap(FlatMapFunction function, int numOperators) {
        FlatMapNode node = new FlatMapNode(function, numOperators);
        addNode(node
        );
        return this;
    }

    public StreamBuilder map(MapFunction function){
        MapNode node = new MapNode(function);
        addNode(node);
        return this;
    }

    public StreamBuilder map(MapFunction function, int numOperators) {
        MapNode node = new MapNode(function, numOperators);
        addNode(node);
        return this;
    }

    public StreamBuilder merge() {
        MergeNode node = new MergeNode();
        addNode(node);
        return this;
    }

    public StreamBuilder merge(int numOperators) {
        MergeNode node = new MergeNode(numOperators);
        addNode(node);
        return this;
    }

    public StreamBuilder split(){
        addNode(new SplitNode());
        return this;
    }

    public StreamBuilder split(int numOperators) {
        addNode(new SplitNode(numOperators));
        return this;
    }

    public StreamBuilder putSink(Sink sink) {
        SinkNode node = new SinkNode(sink);
        addNode(node);
        ok = true;
        return this;
    }

    public void create() {
        create("127.0.0.1", 6123);
    }

    public void create(String ip, int port) {
        if(ok){
            Config conf = ConfigFactory.parseFile(new File("conf/client.conf"));
            ActorSystem sys = ActorSystem.create("Client", conf);

            ActorRef client = sys.actorOf(Client.props(ip, port));
            client.tell(graph, ActorRef.noSender());

        } else throw new IllegalStateException("No Sink added to the graph");
    }

}

class Client extends AbstractActor {

    private final ActorSelection server;


    public Client(String ip, int port) {
        String server_addr = "akka.tcp://Server@" + ip + ":" + port +"/user/ServerActor";
        server = getContext().actorSelection(server_addr);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Graph.class, g -> server.tell(g, self()))
                .match(String.class, m -> m.equals("OK") ,m -> getContext().system().terminate())
                .build();
    }

    public static Props props(String ip, int port) {
        return Props.create(Client.class, ip, port);
    }
}
