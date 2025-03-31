package org.example.electricity;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Mapper Class

public class ElectricityMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private static final Text TOTAL_KEY = new Text("total");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length != 2) {
            return;  // Bỏ qua dòng không hợp lệ
        }

        try {
            int electricityUsage = Integer.parseInt(fields[1].trim());
            float bill = calculateElectricityBill(electricityUsage);
            context.write(TOTAL_KEY, new FloatWritable(bill));
        } catch (NumberFormatException e) {
            // Bỏ qua nếu có lỗi parse số
        }
    }

    private float calculateElectricityBill(int usage) {
        float bill = 0;
        if (usage > 300) {
            bill += (usage - 300) * 2.834f;
            usage = 300;
        }
        if (usage > 200) {
            bill += (usage - 200) * 2.536f;
            usage = 200;
        }
        if (usage > 100) {
            bill += (usage - 100) * 2.014f;
            usage = 100;
        }
        bill += usage * 1.734f;
        return bill;
    }
}