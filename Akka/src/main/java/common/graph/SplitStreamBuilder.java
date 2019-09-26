package main.java.common.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SplitStreamBuilder {
    private final List<StreamBuilder> builders = new ArrayList<>();
    private final StreamBuilder source;

    SplitStreamBuilder(StreamBuilder streamBuilder, SplitNode split, int size){
        source = streamBuilder;
        for(int i =0; i<size; i++) {
            builders.add(new StreamBuilder(split));
        }
    }

    public List<StreamBuilder> getStreamBuilders() {
        return Collections.unmodifiableList(this.builders);
    }

    public StreamBuilder merge(){
        MergeNode merge = new MergeNode();
        for(StreamBuilder builder : builders)
            builder.addMerge(merge);
        source.merge(merge);
        return source;
    }

    public StreamBuilder merge(int numOperators){
        MergeNode merge = new MergeNode(numOperators);
        for(StreamBuilder builder : builders)
            builder.addMerge(merge);
        source.merge(merge);
        return source;
    }
}
