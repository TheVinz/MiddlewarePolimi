package main.java.common.interfaces;

import main.java.common.pair.Pair;

import java.io.Serializable;
import java.util.List;

public interface AggregateFunction extends Serializable {
    Pair aggregate(List<Pair> pairs);
}
