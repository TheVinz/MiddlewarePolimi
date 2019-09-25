package main;

import main.java.common.graph.SplitStreamBuilder;
import main.java.common.graph.StreamBuilder;
import main.java.common.interfaces.AggregateFunction;
import main.java.common.interfaces.FilterFunction;
import main.java.common.interfaces.FlatMapFunction;
import main.java.common.interfaces.MapFunction;
import main.java.common.pair.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Test {
    static Random rnd = new Random();

    public static void main(String[] args) {
        SplitStreamBuilder split =
                (new StreamBuilder(100))
                    .map(new CrashIf0Key())
                    .flatMap(new MyFlatMap())
                    .split(2);

        split.getStreamBuilders().get(0)
                .filter(new OddFilter())
                .map(new MyMap());

        split.getStreamBuilders().get(1)
                .filter(new EvenFilter())
                .map(new DivideBy2());

        split.merge()
                .aggregate(new MyAggregate())
                .putSink()
                .create();
    }

}

class MyMap implements MapFunction {

    @Override
    public Pair map(Pair pair) {
        if(pair.getKey().equals("0"))
            throw new RuntimeException("Crash!!!");
        return new Pair(pair.getKey(), Integer.toString(Integer.valueOf(pair.getValue()) +1));
    }
}

class EvenFilter implements FilterFunction {

    @Override
    public boolean filter(Pair pair) {

        if(Test.rnd.nextInt(10)==0)
            throw new RuntimeException("Crash!!!");
        return Integer.valueOf(pair.getValue())%2 == 0;
    }
}

class OddFilter implements FilterFunction {

    @Override
    public boolean filter(Pair pair){
        if(Test.rnd.nextInt(10) == 0)
            throw new RuntimeException("Crash!!!");
        return Integer.valueOf(pair.getValue())%2 == 1;
    }
}

class MyAggregate implements AggregateFunction {

    @Override
    public Pair aggregate(List<Pair> pairs) {
        if(Integer.valueOf(pairs.get(0).getKey()) % 10 == 0)
            throw new RuntimeException("CRASH!!!");
        int res = 0;
        for(Pair p : pairs)
            res = res + Integer.valueOf(p.getValue());
        return new Pair(pairs.get(0).getKey(), Integer.toString(res));
    }
}

class MyFlatMap implements FlatMapFunction {

    @Override
    public List<Pair> flatMap(Pair pair) {
        if(Test.rnd.nextInt(10)==0)
            throw new RuntimeException("Crash!!!");
        List<Pair> result = new ArrayList<>();
        int value = Integer.valueOf(pair.getValue());
        for(int i = 1; i<50; i++){
            if(value%i == 0)
                result.add(new Pair(pair.getValue(), Integer.toString(value/i)));
        }
        return result;
    }
}

class DivideBy2 implements MapFunction {
    @Override
    public Pair map(Pair pair) {
        if(Test.rnd.nextInt(10) == 0)
            throw new RuntimeException("Crash!!");
        int res = Integer.valueOf(pair.getValue()) /2;
        return new Pair(pair.getKey(), Integer.toString(res));
    }
}

class CrashIf0Key implements MapFunction{
    @Override
    public Pair map(Pair pair) {
        if(pair.getKey().equals("0"))
            throw new RuntimeException("THE KEY IS 0!!!");
        return pair;
    }
}
