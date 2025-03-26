package org.example.airsensor;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer Class
public class COAverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float avgConcentration = 0;
        int count = 0;
        for (FloatWritable value : values) {
             avgConcentration += value.get();
             count++;
        }
        avgConcentration = count > 0 ? avgConcentration / count : avgConcentration;
        context.write(key, new FloatWritable(avgConcentration));
    }
}
