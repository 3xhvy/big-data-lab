package org.example.transaction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PairTransactionMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final int FLUSH_THRESHOLD = 10000;
    private static final int BATCH_SIZE = 100;
    private final Map<String, Integer> countMap = new HashMap<>();
    private final Text keyText = new Text();
    private final IntWritable valueInt = new IntWritable(1);

    private void emitCount(Context context, String key, int count) throws IOException, InterruptedException {
        keyText.set(key);
        valueInt.set(count);
        context.write(keyText, valueInt);
    }

    private void flushCounts(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            emitCount(context, entry.getKey(), entry.getValue());
        }
        countMap.clear();
    }

    private Stream<int[]> generatePairs(int n) {
        return IntStream.range(0, n - 1)
                .boxed()
                .flatMap(i -> IntStream.range(i + 1, n)
                        .mapToObj(j -> new int[]{i, j}));
    }

    private void processBatch(String[] items, int start, int end, Context context) throws IOException, InterruptedException {
        Set<String> batchItems = new HashSet<>(Arrays.asList(Arrays.copyOfRange(items, start, end)));
        String[] itemArray = batchItems.toArray(new String[0]);
        int n = itemArray.length;

        // Count single items
        batchItems.forEach(item -> countMap.merge(item, 1, Integer::sum));

        // Process pairs using streams
        generatePairs(n).forEach(pair -> {
            if(!itemArray[pair[0]].equals(itemArray[pair[1]])) {
                String pairKey = itemArray[pair[0]] + "," + itemArray[pair[1]];
                countMap.merge(pairKey, 1, Integer::sum);
            }
        });

        // Flush if map gets too large
        if (countMap.size() >= FLUSH_THRESHOLD) {
            flushCounts(context);
        }
    }

    private void processOverlappingBatches(String[] items, Context context) throws IOException, InterruptedException {
        int totalItems = items.length;
        int overlap = BATCH_SIZE / 2;
        
        for (int i = 0; i < totalItems; i += (BATCH_SIZE - overlap)) {
            int end = Math.min(i + BATCH_SIZE, totalItems);
            processBatch(items, i, end, context);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().split(" ");
        int itemCount = Math.min(items.length, 100); // Limit the number of items to prevent excessive combinations
        String[] limitedItems = Arrays.copyOf(items, itemCount);
        processOverlappingBatches(limitedItems, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flushCounts(context);
    }
}
