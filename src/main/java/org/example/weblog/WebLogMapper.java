package org.example.weblog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> localMapper;

    @Override
    protected void setup(Context context) {
        localMapper = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        String[] record = value.toString().split(",");
        if(record.length == 2) {
            String url = record[0].trim();
            int count = Integer.parseInt(record[1].trim());

            localMapper.put(url, localMapper.getOrDefault(url, 0) + count);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : localMapper.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }

    }
}
