package main.java.common.interfaces;

import main.java.common.pair.Pair;

import java.io.Serializable;

public interface Sink extends Serializable {
    void putPair(Pair pair);
}
