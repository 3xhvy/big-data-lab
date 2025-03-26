package org.example.sale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Mapper Class
public class SalesMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Map<String, Double> salesMap = new HashMap<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 4) {
            String productID = fields[1];
            double price = Double.parseDouble(fields[2]);
            int quantity = Integer.parseInt(fields[3]);
            double revenue = price * quantity;

            salesMap.put(productID, salesMap.getOrDefault(productID, 0.0) + revenue);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Double> entry : salesMap.entrySet()) {
            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
        }
    }
}
