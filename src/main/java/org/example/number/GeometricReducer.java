package org.example.number;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer Class
public class GeometricReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double logSum = 0;
    private double count = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
        }

        if (key.toString().equals("geometric_mean")) {
            logSum = sum;
        } else if (key.toString().equals("count")) {
            count = sum;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (count > 0) {
            double geometricMean = Math.exp(logSum / count);
            context.write(new Text("Geometric Mean"), new DoubleWritable(geometricMean));
        } else {
            context.write(new Text("Geometric Mean"), new DoubleWritable(0)); // Tránh chia cho 0
        }
    }
}