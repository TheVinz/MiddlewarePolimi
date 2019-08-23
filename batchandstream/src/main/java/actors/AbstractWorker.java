package main.java.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.google.gson.Gson;
import main.java.message.EndMessage;
import main.java.message.Pair;

import java.io.*;
import java.util.List;
import java.util.Optional;

public abstract class AbstractWorker extends AbstractActor {

    class State {
        private Pair pair;
        private int retry = 0;

        public Pair getPair() {
            return pair;
        }

        int getRetry() {
            return retry;
        }

        void increase(){
            retry ++;
            valid();
        }

        public void setPair(Pair pair) {
            this.pair = pair;
        }

        private void valid(){
            if(retry >= Supervisor.MAX_RETRIES) {
                reset();
                System.err.println("RESET!!");
            }
        }

        void reset(){
            retry = 0;
            pair = null;
        }

        boolean isEmpty(){
            return pair == null;
        }
    }

    private boolean closed = false;
    private final File backupFile =  new File(self().toString().replace('/', '_') + "_backup.json");
    private final List<ActorRef> downstramOperators;
    private State state = new State();

    AbstractWorker(){
        downstramOperators = null;
        try {
            boolean check = backupFile.createNewFile();
            if(check)
                System.out.println("Backup file created for " + self());
        } catch (IOException e) {
            System.err.println("Error on create new file");
        }
    }

    AbstractWorker(List<ActorRef> downstramOperators) {
        this.downstramOperators = downstramOperators;
        try {
            boolean check = backupFile.createNewFile();
            if(check)
                System.out.println("Backup file created for " + self());
        } catch (IOException e) {
            System.err.println("Error on create new file");
        }
    }

    boolean isClosed() {
        return closed;
    }

    void close(){
        closed = true;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Pair.class, this::onPair)
                .match(EndMessage.class, m -> {
                    if(!closed) {
                        downstramOperators.forEach(a -> a.tell(m, self()));
                        close();
                    }
                })
                .build();
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

        if(!state.isEmpty()){
            onPair(state.getPair());
        }
    }

    @Override
    public void postStop() {
        boolean check = backupFile.delete();
        if(!check) {
            System.err.println("Something wend wrong on deleting backup file on " + self());
        }
    }

    void setCurrent(Pair current) {
        state.setPair(current);
    }

    List<ActorRef> getDownstramOperators() {
        return downstramOperators;
    }

    File getBackupFile() {
        return backupFile;
    }

    private void saveState(){
        Gson gson = new Gson();
        String json = gson.toJson(state);
        if(! backupFile.exists()) {
            System.err.println("Backup file does not exist!!!");
            return;
        }
        try(PrintWriter writer = new PrintWriter(new FileWriter(backupFile))){
            writer.println(json);
        } catch (IOException e) {
            System.err.println("File write failed");
        }
    }

    private void loadState(){
        Gson gson = new Gson();
        if(! backupFile.exists()) {
            System.err.println("Backup file does not exist!!!");
            return;
        }
        try(BufferedReader reader = new BufferedReader(new FileReader(backupFile))){
            String json = reader.readLine();
            state = gson.fromJson(json, State.class);
            System.err.println(getSelf() + " retry " + state.getRetry());
        } catch (IOException e) {
            System.err.println("File read failed");
        }
    }
    abstract void onPair(Pair pair);

}
