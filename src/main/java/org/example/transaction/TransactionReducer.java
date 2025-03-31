package org.example.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import java.util.HashMap;
import java.util.Map;

public class TransactionReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Map<String, Integer> singleCounts = new HashMap<>();
    private final Map<String, Integer> pairCounts = new HashMap<>();
    private final Map<String, Integer> tripletCounts = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        String[] parts = key.toString().split(",");
        if (parts.length == 1) {
            singleCounts.put(key.toString(), sum);
        } else if (parts.length == 2) {
            pairCounts.put(key.toString(), sum);
        } else if (parts.length == 3) {
            tripletCounts.put(key.toString(), sum);
        }

        context.write(key, new Text("Count: " + sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
            String pair = entry.getKey();
            int countAB = entry.getValue();
            String[] products = pair.split(",");
            String A = products[0];

            if (singleCounts.containsKey(A)) {
                double prob = (double) countAB / singleCounts.get(A);
                context.write(new Text("P(" + products[1] + " | " + A + ")"), new Text(String.format("%.5f", prob)));
            }
        }

        for (Map.Entry<String, Integer> entry : tripletCounts.entrySet()) {
            String triplet = entry.getKey();
            int countABC = entry.getValue();
            String[] products = triplet.split(",");
            String B = products[1];
            String C = products[2];

            String pairBC = B + "," + C;
            if (pairCounts.containsKey(pairBC)) {
                double prob = (double) countABC / pairCounts.get(pairBC);
                context.write(new Text("P(" + products[0] + " | " + B + "," + C + ")"), new Text(String.format("%.5f", prob)));
            }
        }
    }
}
