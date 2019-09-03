package main;

import main.java.client.StreamBuilder;
import main.java.common.interfaces.MapFunction;
import main.java.common.interfaces.Sink;
import main.java.common.pair.Pair;

public class Test {
    public static void main(String[] args) {
        (new StreamBuilder(100))
                .map(new MyMap())
                .putSink(new MySink())
                .create();
    }

}

class MyMap implements MapFunction {

    @Override
    public Pair map(Pair pair) {
        return new Pair(pair.getKey(), Integer.toString(Integer.valueOf(pair.getValue()) +1));
    }
}

class MySink implements Sink {

    @Override
    public void putPair(Pair pair) {
        System.out.println(pair);
    }
}
