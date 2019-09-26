package main.java.common.graph;

import akka.actor.*;
import com.google.gson.Gson;
import com.typesafe.config.ConfigFactory;
import main.java.common.interfaces.*;
import com.typesafe.config.Config;


import java.io.*;
import java.time.Duration;

public class StreamBuilder {

    private final Graph graph;
    private boolean complete = false;
    private boolean isSplit;
    private boolean locked = false;
    private final boolean closeable;
    private Node current;

    public StreamBuilder(long period){
        SourceNode node = new SourceNode(period);
        this.graph = new Graph(node);
        this.current = node;
        isSplit = false;
        closeable = true;
    }

    StreamBuilder(Node node) {
        graph = null;
        this.current = node;
        this.isSplit = true;
        closeable = false;
    }

    private void addNode(Node node){
        if(complete)
            throw new RuntimeException("Cannot add anything after Sink.");
        else if(locked)
            throw  new RuntimeException("Invalid operation.");
        current.setNext(node);
        current = node;
    }

    void addMerge(MergeNode node) {
        if(isSplit)
            current.setNext(node);
        else if(locked)
            throw new RuntimeException("Invalid operation.");
        else
            throw new RuntimeException("Merging without a Split.");
    }

    void merge(MergeNode node){
        if(current instanceof SplitNode){
            ((SplitNode) current).merge();
            current = node;
            isSplit = !closeable;
            locked = false;
        } else
            throw new RuntimeException("Merging without split");
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

    public SplitStreamBuilder split(int streamNumber){
        SplitNode node = new SplitNode();
        addNode(node);
        isSplit = true;
        locked = true;
        return new SplitStreamBuilder(this, node, streamNumber);
    }

    public SplitStreamBuilder split(int numOperators, int streamNumber) {
        SplitNode node = new SplitNode(numOperators);
        addNode(node);
        isSplit = true;
        locked = true;
        return new SplitStreamBuilder(this, node, streamNumber);
    }

    public StreamBuilder putSink() {
        if(isSplit)
            throw new RuntimeException("Cannot add Sink in unmerged streams.");
        SinkNode node = new SinkNode();
        addNode(node);
        complete = true;
        return this;
    }

    public void create() {
        Server server;
        if(complete){
            try(BufferedReader reader = new BufferedReader(new FileReader(new File("conf/commonconf/server.json")))){
                String line = reader.readLine();
                server = (new Gson()).fromJson(line, Server.class);
            } catch (FileNotFoundException e) {
                System.err.println("File server.json not found in conf/commonconf directory.");
                return;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            Config conf = ConfigFactory.parseFile(new File("conf/clientconf/client.conf"));
            ActorSystem sys = ActorSystem.create("Client", conf);

            ActorRef client = sys.actorOf(Client.props(server.ip, server.port));
            client.tell(graph, ActorRef.noSender());

        } else throw new IllegalStateException("No Sink added to the graph");
    }

    public void print(){
        System.out.println(this.graph);
    }

}

class Client extends AbstractActor {

    private final ActorSelection server;


    public Client(String ip, int port) {
        String server_addr = "akka.tcp://Server@" + ip + ":" + port +"/user/ServerActor";
        server = getContext().actorSelection(server_addr);
        getContext().setReceiveTimeout(Duration.ofSeconds(5));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Graph.class, g -> server.tell(g, self()))
                .match(String.class, m -> m.equals("OK") ,m -> getContext().system().terminate())
                .match(ReceiveTimeout.class, m -> {
                    System.err.println("Server timed out.");
                    getContext().system().terminate();
                })
                .build();
    }

    public static Props props(String ip, int port) {
        return Props.create(Client.class, ip, port);
    }
}

class Server {
    int port = 6123;
    String ip = "localhost";
}
