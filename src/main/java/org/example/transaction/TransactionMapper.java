package org.example.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
public class TransactionMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Map<String, Integer> countMap = new HashMap<>();

    @Override
    protected void map(Object key, Text value, Context context) {
        String[] items = value.toString().split(" ");
        Set<String> uniqueItems = new HashSet<>();
        for (String item : items) {
            uniqueItems.add(item);
        }

        // Count single product occurrences
        for (String itemA : uniqueItems) {
            countMap.put(itemA, countMap.getOrDefault(itemA, 0) + 1);
        }

        // Count product pairs (A, B)
        for (String itemA : uniqueItems) {
            for (String itemB : uniqueItems) {
                if (!itemA.equals(itemB)) {
                    String pair = itemA + "," + itemB;
                    countMap.put(pair, countMap.getOrDefault(pair, 0) + 1);
                }
            }
        }

        // Count product triplets (A, B, C)
        for (String itemA : uniqueItems) {
            for (String itemB : uniqueItems) {
                for (String itemC : uniqueItems) {
                    if (!itemA.equals(itemB) && !itemA.equals(itemC) && !itemB.equals(itemC)) {
                        String triplet = itemA + "," + itemB + "," + itemC;
                        countMap.put(triplet, countMap.getOrDefault(triplet, 0) + 1);
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable countWritable = new IntWritable();
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            countWritable.set(entry.getValue());
            context.write(new Text(entry.getKey()), countWritable);
        }
    }
}

