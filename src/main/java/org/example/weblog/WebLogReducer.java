package org.example.weblog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WebLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int totalDuration = 0;
        for(IntWritable value : values) {
            totalDuration += value.get();
        }

        try {
            context.write(key, new IntWritable(totalDuration));
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }


}
