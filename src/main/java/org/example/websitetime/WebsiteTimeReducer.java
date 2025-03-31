package org.example.websitetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer Class
public class WebsiteTimeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Text maxSite = new Text();
    private Text minSite = new Text();
    private long maxTime = Long.MIN_VALUE;
    private long minTime = Long.MAX_VALUE;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalTime = 0;
        for (LongWritable val : values) {
            totalTime += val.get();
        }

        if (totalTime > maxTime) {
            maxTime = totalTime;
            maxSite.set(key);
        }

        if (totalTime < minTime) {
            minTime = totalTime;
            minSite.set(key);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Most Viewed: " + maxSite), new LongWritable(maxTime));
        context.write(new Text("Least Viewed: " + minSite), new LongWritable(minTime));
    }
}