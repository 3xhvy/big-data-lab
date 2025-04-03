package org.example.marketprice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarketPriceDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MarketPriceDriver <input path> <output path> <price threshold>");
            return -1;
        }

        Configuration conf = getConf();
        conf.setDouble("priceThreshold", Double.parseDouble(args[2]));

        Job job = Job.getInstance(conf, "Market Price Analysis");
        job.setJarByClass(MarketPriceDriver.class);

        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(MarketPriceMapper.class);
        job.setCombinerClass(MarketPriceReducer.class);  // Using Reducer as Combiner since we're doing sum
        job.setReducerClass(MarketPriceReducer.class);

        // Set output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set number of reducers
        job.setNumReduceTasks(2);  // Adjust based on your data size

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MarketPriceDriver(), args);
        System.exit(exitCode);
    }
}
