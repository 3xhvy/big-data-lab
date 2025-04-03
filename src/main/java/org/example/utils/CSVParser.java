package org.example.utils;

public class CSVParser {
    public static String[] parseCSVLine(String line) {
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
}
