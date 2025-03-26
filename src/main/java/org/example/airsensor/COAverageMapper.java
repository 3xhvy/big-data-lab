package org.example.airsensor;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Mapper Class
public class COAverageMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text mapperKey = new Text();
    private FloatWritable mapperValue = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 3) {
            try {
                String sensorId = fields[0].trim();
                String gasType = fields[1].trim();
                Float concentration = Float.parseFloat(fields[2].trim());

                if(gasType.equals("CO")) {
                    mapperKey.set(sensorId);
                    mapperValue.set(concentration);
                    context.write(mapperKey, mapperValue);
                }
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
