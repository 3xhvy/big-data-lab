package org.example.number;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper Class
public class GeometricMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final Text mapperKey = new Text("geometric_mean");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            double number = Double.parseDouble(value.toString());
            if(number > 0) {
                context.write(mapperKey, new DoubleWritable((float) Math.log(number)));
                context.write(new Text("count"), new DoubleWritable(1));
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
