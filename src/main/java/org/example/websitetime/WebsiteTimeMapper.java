package org.example.websitetime;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper Class
public class WebsiteTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length != 2) {
            return;  // Bỏ qua dòng không hợp lệ
        }

        try {
            String url = fields[0].trim();
            long timeSpent = Long.parseLong(fields[1].trim());
            context.write(new Text(url), new LongWritable(timeSpent));
        } catch (NumberFormatException e) {
            // Bỏ qua nếu có lỗi parse số
        }
    }
}