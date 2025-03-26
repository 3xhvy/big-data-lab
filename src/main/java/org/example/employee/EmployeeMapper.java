package org.example.employee;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(", ");
        if (fields.length != 5) {
            return;  // Bỏ qua dòng không hợp lệ
        }

        String fullName = fields[0] + " " + fields[1]; // Họ + Tên
        String department = fields[3]; // Phòng ban
        String salary = fields[4]; // Lương

        context.write(new Text(department), new Text(fullName + ":" + salary));
    }
}
