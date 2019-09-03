package main.java.common.interfaces;

import main.java.common.pair.Pair;

import java.io.Serializable;

public interface FilterFunction extends Serializable {
    boolean filter(Pair pair);
}
