package org.example.electricity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer Class
public class ElectricityReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float totalBill = 0;
        for (FloatWritable val : values) {
            totalBill += val.get();
        }
        context.write(new Text("Total Revenue"), new FloatWritable(totalBill));
    }
}