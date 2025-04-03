package org.example.marketprice.minmax;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MinMaxPriceReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        for (Text val : values) {
            double price = Double.parseDouble(val.toString());
            min = Math.min(min, price);
            max = Math.max(max, price);
        }

        result.set(String.format("Min: %.2f, Max: %.2f", min, max));
        context.write(key, result);
    }
}
