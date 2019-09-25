package main.java.common.interfaces;

import main.java.common.pair.Pair;

import java.io.Serializable;

public interface MapFunction extends Serializable {
    Pair map(Pair pair);
}
