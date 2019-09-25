package main.java.common.interfaces;

import main.java.common.pair.Pair;

import java.io.Serializable;
import java.util.List;

public interface FlatMapFunction extends Serializable {
    List<Pair> flatMap(Pair pair);
}
