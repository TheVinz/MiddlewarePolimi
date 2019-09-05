package main;

import main.java.client.StreamBuilder;
import main.java.common.interfaces.AggregateFunction;
import main.java.common.interfaces.FilterFunction;
import main.java.common.interfaces.FlatMapFunction;
import main.java.common.interfaces.MapFunction;
import main.java.common.pair.Pair;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        (new StreamBuilder(100))
                .map(new MyMap())
                .split()
                .aggregate(new MyAggregate(), 8, 2)
                .flatMap(new MyFlatMap())
                .merge()
                .putSink()
                .create();
    }

}

class MyMap implements MapFunction {

    @Override
    public Pair map(Pair pair) {
        return new Pair(pair.getKey(), Integer.toString(Integer.valueOf(pair.getValue()) +1));
    }
}

class MyFilter implements FilterFunction {

    @Override
    public boolean filter(Pair pair) {
        return Integer.valueOf(pair.getValue())%2 == 0;
    }
}

class MyAggregate implements AggregateFunction {

    @Override
    public Pair aggregate(List<Pair> pairs) {
        int res = 0;
        for(Pair p : pairs)
            res = res + Integer.valueOf(p.getValue());
        return new Pair(Integer.toString(res), Integer.toString(res));
    }
}

class MyFlatMap implements FlatMapFunction {

    @Override
    public List<Pair> flatMap(Pair pair) {
        List<Pair> result = new ArrayList<>();
        int value = Integer.valueOf(pair.getValue());
        for(int i = 1; i<50; i++){
            if(value%i == 0)
                result.add(new Pair(pair.getValue(), Integer.toString(value/i)));
        }
        return result;
    }
}
