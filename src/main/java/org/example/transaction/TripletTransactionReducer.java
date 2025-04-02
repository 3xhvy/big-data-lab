package org.example.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import java.util.*;

public class TripletTransactionReducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final int FLUSH_THRESHOLD = 10000;
    private final Map<String, Integer> singleCounts = new HashMap<>();
    private final Map<String, TripletInfo> tripletCounts = new HashMap<>();
    
    private static class TripletInfo {
        final String b;
        final String c;
        final int count;
        
        TripletInfo(String b, String c, int count) {
            this.b = b;
            this.c = c;
            this.count = count;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        String[] parts = key.toString().split(",");
        if (parts.length == 1) {
            singleCounts.put(key.toString(), sum);
            // Flush if singles map gets too large
            if (singleCounts.size() >= FLUSH_THRESHOLD) {
                processTriplets(context);
                singleCounts.clear();
            }
        } else if (parts.length == 3) {
            tripletCounts.put(parts[0], new TripletInfo(parts[1], parts[2], sum));
            // Flush if triplets map gets too large
            if (tripletCounts.size() >= FLUSH_THRESHOLD) {
                processTriplets(context);
                tripletCounts.clear();
            }
        }
    }

    private void processTriplets(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, TripletInfo> entry : tripletCounts.entrySet()) {
            String A = entry.getKey();
            TripletInfo info = entry.getValue();
            
            if (singleCounts.containsKey(A)) {
                double probBC_A = (double) info.count / singleCounts.get(A);
                context.write(
                    new Text("Count A: " + singleCounts.get(A) + ", Count ABC: " + info.count + 
                           ", P(" + info.b + "," + info.c + " | " + A + ")"),
                    new Text(String.format("%.5f", probBC_A))
                );
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        processTriplets(context);
        // Clear maps to help GC
        tripletCounts.clear();
        singleCounts.clear();
    }
}
