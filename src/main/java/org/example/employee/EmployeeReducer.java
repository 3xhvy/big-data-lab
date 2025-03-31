package org.example.employee;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class EmployeeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String highestPaidEmployee = "";
        String lowestPaidEmployee = "";
        long maxSalary = Long.MIN_VALUE;
        long minSalary = Long.MAX_VALUE;

        for (Text val : values) {
            count++;
            String[] parts = val.toString().split(":");
            String name = parts[0];
            long salary = Long.parseLong(parts[1]);

            if (salary > maxSalary) {
                maxSalary = salary;
                highestPaidEmployee = name;
            }

            if (salary < minSalary) {
                minSalary = salary;
                lowestPaidEmployee = name;
            }
        }

        String result = "Total: " + count +
                ", Highest: " + highestPaidEmployee + " (" + maxSalary + ")" +
                ", Lowest: " + lowestPaidEmployee + " (" + minSalary + ")";
        context.write(key, new Text(result));
    }
}
