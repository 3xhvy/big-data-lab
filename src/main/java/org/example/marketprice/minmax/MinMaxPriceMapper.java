package org.example.marketprice.minmax;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.utils.CSVParser;
import java.io.IOException;

public class MinMaxPriceMapper extends Mapper<Object, Text, Text, Text> {
    private final Text productKey = new Text();
    private final Text priceValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = CSVParser.parseCSVLine(line);
        
        if (parts.length >= 4 && !parts[0].equals("ten")) {
            try {
                String product = parts[0];
                double price = Double.parseDouble(parts[1]);
                
                productKey.set(product);
                priceValue.set(String.valueOf(price));
                context.write(productKey, priceValue);
            } catch (NumberFormatException e) {
                context.getCounter("MinMax Price", "Invalid Price Records").increment(1);
            }
        }
    }
}
