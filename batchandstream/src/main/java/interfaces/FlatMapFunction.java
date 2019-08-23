package main.java.interfaces;

import main.java.message.Pair;

import java.util.List;

public interface FlatMapFunction{
    List<Pair> flatMap(Pair pair);
}
