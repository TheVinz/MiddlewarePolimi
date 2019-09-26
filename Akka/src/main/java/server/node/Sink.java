package main.java.server.node;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import main.java.common.pair.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Sink {

    private static final int LIMIT = 200;

    private final Gson gson = new Gson();
    private final File file = new File("Output.json");
    private final List<Pair> out = new ArrayList<>();
    private int count = 0;

    public Sink() {
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putPair(Pair pair){
        System.out.println("Sink >>> " + pair);
       out.add(pair);
       count++;
       if(count == LIMIT){
           flush();
       }
    }

    public synchronized void flush(){
        count = 0;
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))){
            for (Pair p : out) {
                writer.write(gson.toJson(p) + "\n");
            }
            out.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void close(){
        flush();
        file.delete();
    }

    public synchronized JsonArray getCurrentOutput(){
        String json;
        JsonArray result = new JsonArray();
        JsonElement tmp;
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            while((json = reader.readLine()) != null){
                tmp = gson.fromJson(json, JsonElement.class);
                result.add(tmp);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        for(Pair p : out) {
            tmp = gson.toJsonTree(p);
            result.add(tmp);
        }
        return result;
    }
}
