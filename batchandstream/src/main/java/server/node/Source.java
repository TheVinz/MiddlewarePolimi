package main.java.server.node;

import main.java.common.pair.Pair;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

public class Source {

    private final Queue<Pair> pairs = new ArrayDeque<>();
    private boolean isClosed = false;

    public synchronized Pair getPair() {
        while (pairs.isEmpty() && !isClosed) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if(isClosed)
            return null;
        else
            return pairs.poll();
    }

    public synchronized void put(Pair p) {
        pairs.add(p);
        notifyAll();
    }

    public synchronized void put(Collection<Pair> pairs) {
        this.pairs.addAll(pairs);
        notifyAll();
    }

    public synchronized void close() {
        this.isClosed = true;
        notifyAll();
    }

    public int getQueueSize(){
        return pairs.size();
    }
}
