package org.example.marketprice.average;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AveragePriceReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            sum += Double.parseDouble(parts[0]);
            count += Integer.parseInt(parts[1]);
        }

        double average = sum / count;
        result.set(String.format("%.2f", average));
        context.write(key, result);
    }
}
