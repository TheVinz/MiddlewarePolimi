package main.java;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import main.java.actors.EngineActor;
import main.java.graph.*;
import main.java.interfaces.*;
import main.java.graph.Graph;
import main.java.message.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RunServer {

    private static void check(){
        if (Utils.getRandomInt(100) == 0)
            throw new RuntimeException("Crash!!!");
    }

    public static void main(String[] args) throws IOException {
        ActorSystem sys = ActorSystem.create("System");
        ActorRef server = sys.actorOf(EngineActor.props(), "Server");

        SourceNode sourceNode = new SourceNode(new MySource(), 50);
        SinkNode sinkNode = new SinkNode(new MySink());

        FilterFunction even = p -> {
            check();
            return Integer.valueOf(p.getValue()) % 2 == 0;
        };

        FilterFunction odd = p -> {
            check();
            return Integer.valueOf(p.getValue()) % 2 != 0;
        };

        AggregateFunction sum = pairs -> {
            check();
            int res = pairs.stream().mapToInt(p -> Integer.valueOf(p.getValue())).sum();
            return new Pair(pairs.get(0).getKey(), Integer.toString(res));
        };
        FlatMapFunction something = pair -> {
            check();
            int val = Integer.valueOf(pair.getValue());
            List<Pair> res = new ArrayList<>();
            for(int i=1; i<50; i++){
                if(val%i == 0)
                    res.add(new Pair(Integer.toString(val), Integer.toString(val/i)));
            }
            return res;
        };
        MapFunction divideByKey = p -> {
            check();
            return new Pair(p.getKey(), Integer.toString(
                    Integer.valueOf(p.getValue()) / Integer.valueOf(p.getKey())
            ));
        };

        MapNode mapNode = new MapNode(divideByKey);
        SplitNode split = new SplitNode();
        FilterNode evenFilter = new FilterNode(even);
        FilterNode oddFilter = new FilterNode(odd);
        MergeNode merge = new MergeNode();
        AggregateNode packNode = new AggregateNode(sum, 7, 4);
        FlatMapNode unpackNode = new FlatMapNode(something);



        sourceNode.setNext(mapNode);
        mapNode.setNext(split);
        split.addNext(evenFilter);
        split.addNext(oddFilter);
        evenFilter.setNext(merge);
        oddFilter.setNext(merge);
        merge.setNext(packNode);
        packNode.setNext(unpackNode);
        unpackNode.setNext(sinkNode);

        Graph graph = new Graph(sourceNode);

        System.out.println(graph.toString());

        server.tell(graph, ActorRef.noSender());

        System.in.read();
        sys.terminate();

    }
}

class MySource implements Source{

    private Random rnd = new Random(1996L);
    private int curr = 0;


    @Override
    public Pair getPair() {
        if(curr == 500)
            return null;
        int key = rnd.nextInt(40);
        String k = Integer.toString(key);
        String v = Integer.toString(curr++);
        return new Pair(k,v);
    }
}

class MySink implements Sink {

    private int count = 0;

    @Override
    public void putPair(Pair pair) {
        System.out.println(count++ + "  ->  " + pair);
    }
}
