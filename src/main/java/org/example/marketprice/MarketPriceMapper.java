package org.example.marketprice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class MarketPriceMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final int FLUSH_THRESHOLD = 10000;
    private final Map<String, Integer> countMap = new HashMap<>();
    private final Text keyText = new Text();
    private final IntWritable valueInt = new IntWritable(1);
    private double priceThreshold;

    public MarketPriceMapper() {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        priceThreshold = conf.getDouble("priceThreshold", 1000000);
    }

    private String[] parseCSVLine(String line) {
        if (line == null || line.isEmpty()) {
            return new String[0];
        }

        boolean inQuotes = false;
        StringBuilder field = new StringBuilder();
        java.util.ArrayList<String> fields = new java.util.ArrayList<>();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(field.toString().trim().replace("\"", ""));
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        fields.add(field.toString().trim().replace("\"", ""));

        return fields.toArray(new String[0]);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            String[] parts = parseCSVLine(line);
            if (parts.length >= 4) {
                if (parts[0].equals("ten")) return; // Skip header

                String date = parts[3].replace(" 00:00:00", "").trim();
                try {
                    double price = Double.parseDouble(parts[1]);
                    if (price > priceThreshold) {
                        countMap.merge(date, 1, Integer::sum);
                    }
                } catch (NumberFormatException e) {
                    context.getCounter("Market Price", "Invalid Price Records").increment(1);
                }
            }
        } catch (Exception e) {
            context.getCounter("Market Price", "Parse Error Records").increment(1);
        }

        if(countMap.size() >= FLUSH_THRESHOLD) {
            flushCounts(context);
        }
    }

    private void flushCounts(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            keyText.set(entry.getKey());
            valueInt.set(entry.getValue());
            context.write(keyText, valueInt);
        }
        countMap.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        flushCounts(context);
    }
}
