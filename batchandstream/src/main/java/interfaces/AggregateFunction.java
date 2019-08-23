package main.java.interfaces;

import main.java.message.Pair;

import java.util.List;

public interface AggregateFunction {
    Pair aggregate(List<Pair> pairs);
}
