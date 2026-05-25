package org.davidmoten.Experiment;

import org.davidmoten.Scheme.KDTSKQ.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class DataPreprocessor {

    public static List<Record> loadFromCSV(String filepath) throws Exception {
        List<Record> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            boolean isFirstLine = true;
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }
                String[] fields = line.split(",");
                if (fields.length < 4) continue;

                Record r = new Record();
                r.id = Integer.parseInt(fields[0].trim());
                r.x = Integer.parseInt(fields[1].trim());
                r.y = Integer.parseInt(fields[2].trim());
                r.timestamp = Integer.parseInt(fields[3].trim());
                r.W = new ArrayList<>();
                if (fields.length > 4) {
                    String[] kws = fields[4].split(";");
                    for (String kw : kws) {
                        String trimmed = kw.trim();
                        if (!trimmed.isEmpty()) r.W.add(trimmed);
                    }
                }
                records.add(r);
            }
        }
        return records;
    }

    public static List<Record> generateSynthetic(int count, int coordMax, int timeMax, int keywordCount, long seed) {
        java.util.Random rnd = new java.util.Random(seed);
        List<Record> records = new ArrayList<>();
        String[] keywordPool = new String[keywordCount];
        for (int i = 0; i < keywordCount; i++) {
            keywordPool[i] = "kw" + i;
        }
        for (int i = 0; i < count; i++) {
            Record r = new Record();
            r.id = i;
            r.x = rnd.nextInt(coordMax);
            r.y = rnd.nextInt(coordMax);
            r.timestamp = rnd.nextInt(timeMax);
            r.W = new ArrayList<>();
            int numKw = 1 + rnd.nextInt(3);
            for (int j = 0; j < numKw; j++) {
                r.W.add(keywordPool[rnd.nextInt(keywordCount)]);
            }
            records.add(r);
        }
        return records;
    }

    public static int[] getSpatialBounds(List<Record> dataset) {
        if (dataset == null || dataset.isEmpty()) return new int[]{0, 0, 0, 0};
        int minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE, maxY = Integer.MIN_VALUE;
        for (Record r : dataset) {
            if (r.x < minX) minX = r.x;
            if (r.x > maxX) maxX = r.x;
            if (r.y < minY) minY = r.y;
            if (r.y > maxY) maxY = r.y;
        }
        return new int[]{minX, maxX, minY, maxY};
    }
}
