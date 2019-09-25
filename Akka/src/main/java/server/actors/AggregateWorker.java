package main.java.server.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.google.gson.Gson;
import main.java.common.interfaces.AggregateFunction;
import main.java.common.pair.Pair;

import java.io.*;
import java.util.*;

public class AggregateWorker extends AbstractWorker {

    class State {

        private final Map<String, List<Pair>> windows = new HashMap<>();
        private int retry;

        int getRetry() {
            return retry;
        }

        Map<String, List<Pair>> getWindows() {
            return windows;
        }

        void reset(){
            this.retry = 0;
        }

        private void valid(){
            if(this.retry >= Supervisor.MAX_RETRIES){
                windows.clear();
                retry = 0;
                System.out.println("RESET!!");
            }
        }

        void increase(){
            this.retry++;
            valid();
        }
    }

    private final int windowSize;
    private final int windowSlide;
    private final AggregateFunction function;
    private State state = new State();

    public AggregateWorker(AggregateFunction function, int windowSize, int windowSlide, List<ActorRef> downstreamOperators) {
        super(downstreamOperators);
        this.windowSlide = windowSlide;
        this.windowSize = windowSize;
        this.function = function;
    }

    void onPair(Pair pair){
        List<Pair> window = state.getWindows().computeIfAbsent(pair.getKey(), k -> new ArrayList<>());
        window.add(pair);
        if(window.size() == windowSize) {
            clearWindow(window);
        }
        state.reset();
    }


    private void clearWindow(List<Pair> window) {
        Pair res = function.aggregate(window);
        getDownstramOperators()
                .get(res.getKey().hashCode() % getDownstramOperators().size())
                .tell(res, self());
        window.removeAll(window.subList(0, windowSlide));
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        saveState();
    }

    @Override
    public void postRestart(Throwable reason) {
        loadState();
        state.increase();
        saveState();
        for(List<Pair> window : state.getWindows().values())
            if(window.size() >= windowSize)
                clearWindow(window);
    }

    private void saveState(){
        Gson gson = new Gson();
        String json = gson.toJson(state);
        if(! getBackupFile().exists()) {
            System.err.println("Backup file does not exist!!!");
            return;
        }
        try(PrintWriter writer = new PrintWriter(new FileWriter(getBackupFile()))){
            writer.println(json);
        } catch (IOException e) {
            System.err.println("File write failed");
        }
    }

    private void loadState(){
        Gson gson = new Gson();
        if(! getBackupFile().exists()) {
            System.err.println("Backup file does not exist!!!");
            return;
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(getBackupFile()))){
            String json = reader.readLine();
            state = gson.fromJson(json, State.class);
            System.err.println(getSelf() + " retry " + state.getRetry());
        } catch (IOException e) {
            System.err.println("File read failed");
        }
    }


    public static Props props(AggregateFunction function, int windowSize, int windowSlide, List<ActorRef> downstreamOperators){
        return Props.create(AggregateWorker.class, function, windowSize, windowSlide, downstreamOperators);
    }
}

