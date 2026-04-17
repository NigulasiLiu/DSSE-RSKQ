package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * EPSRQ 对齐实验 2:
 * 对齐 Exp_All_Schemes_Var_H_R_N 里的三类实验:
 * - C&D: h 变化 (Update 与 Search)
 * - E: R 变化 (Search)
 * - F: N 变化 (Search)
 *
 * 参数语义映射:
 * - h -> T
 * - l(固定 2^20) -> gamma(固定 2^20, 同时作为 maxFiles)
 */
public final class EPSRQ_Exp_Var_H_R_N {

    private EPSRQ_Exp_Var_H_R_N() {
    }

    public static void main(String[] args) throws Exception {
        String basePath = "src/dataset/spatial_data_set_";
        int fixedL = 1 << 20;
        int div = 100;
        int warmUpTimes = 9000;
        int formalTimes = 1000;
        int totalLoops = warmUpTimes + formalTimes;
        long seed = 20260105L;
        Random random = new Random(seed);

        int[] hValues = {8, 10, 12};
        int[] rValues = {1, 5, 10, 15, 20};
        String[] nLabels = {"2W", "4W", "6W", "8W", "10W"};

        double[] cUpdate = new double[hValues.length];
        double[] dSearch = new double[hValues.length];
        double[] eSearch = new double[rValues.length];
        double[] fSearch = new double[nLabels.length];

        List<FixRangeCompareToConstructionOne.DataRow> raw10W =
                FixRangeCompareToConstructionOne.loadDataFromFile(basePath + "10W.csv");

        for (int i = 0; i < hValues.length; i++) {
            int h = hValues[i];
            List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw10W, h);
            EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, h, fixedL, seed);

            long upStart = System.nanoTime();
            for (FixRangeCompareToConstructionOne.DataRow row : data) {
                epsrq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID % fixedL});
            }
            long upEnd = System.nanoTime();
            cUpdate[i] = (upEnd - upStart) / 1e6;

            dSearch[i] = runSearch(epsrq, data, h, 3, div, random, totalLoops, warmUpTimes, formalTimes);
        }

        int fixedH = 10;
        List<FixRangeCompareToConstructionOne.DataRow> fixedData = normalizeData(raw10W, fixedH);
        EPSRQ_Adapter epsrqFixed = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
        for (FixRangeCompareToConstructionOne.DataRow row : fixedData) {
            epsrqFixed.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID % fixedL});
        }
        for (int i = 0; i < rValues.length; i++) {
            eSearch[i] = runSearch(epsrqFixed, fixedData, fixedH, rValues[i], div, random, totalLoops, warmUpTimes, formalTimes);
        }

        for (int i = 0; i < nLabels.length; i++) {
            String label = nLabels[i];
            List<FixRangeCompareToConstructionOne.DataRow> raw =
                    FixRangeCompareToConstructionOne.loadDataFromFile(basePath + label + ".csv");
            List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw, fixedH);
            EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
            for (FixRangeCompareToConstructionOne.DataRow row : data) {
                epsrq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID % fixedL});
            }
            fSearch[i] = runSearch(epsrq, data, fixedH, 3, div, random, totalLoops, warmUpTimes, formalTimes);
        }

        writeResults("experiment_epsrq_2_var_h_r_n.txt", hValues, rValues, nLabels, cUpdate, dSearch, eSearch, fSearch);
    }

    private static double runSearch(EPSRQ_Adapter epsrq,
                                    List<FixRangeCompareToConstructionOne.DataRow> data,
                                    int h,
                                    int rPercent,
                                    int div,
                                    Random random,
                                    int totalLoops,
                                    int warmUp,
                                    int formalTimes) {
        long sumNs = 0L;
        int edge = 1 << h;
        int rangeLen = Math.max(1, edge * rPercent / div);
        int n = data.size();

        for (int i = 0; i < totalLoops; i++) {
            int xStart = random.nextInt(Math.max(1, edge - rangeLen));
            int yStart = random.nextInt(Math.max(1, edge - rangeLen));
            FixRangeCompareToConstructionOne.DataRow row = data.get(random.nextInt(n));

            long t0 = System.nanoTime();
            epsrq.searchRect(xStart, yStart, rangeLen, row.keywords);
            long t1 = System.nanoTime();
            if (i >= warmUp) {
                sumNs += (t1 - t0);
            }
        }
        return (sumNs / 1e6) / formalTimes;
    }

    private static List<FixRangeCompareToConstructionOne.DataRow> normalizeData(
            List<FixRangeCompareToConstructionOne.DataRow> rawData, int h) {
        long maxX = 0L;
        long maxY = 0L;
        for (FixRangeCompareToConstructionOne.DataRow row : rawData) {
            if (row.pointX > maxX) {
                maxX = row.pointX;
            }
            if (row.pointY > maxY) {
                maxY = row.pointY;
            }
        }
        if (maxX == 0) {
            maxX = 1;
        }
        if (maxY == 0) {
            maxY = 1;
        }

        long maxVal = (1L << h) - 1;
        List<FixRangeCompareToConstructionOne.DataRow> scaled = new java.util.ArrayList<>(rawData.size());
        for (FixRangeCompareToConstructionOne.DataRow row : rawData) {
            long newX = (row.pointX * maxVal) / maxX;
            long newY = (row.pointY * maxVal) / maxY;
            scaled.add(new FixRangeCompareToConstructionOne.DataRow(row.fileID, newX, newY, row.keywords));
        }
        return scaled;
    }

    private static void writeResults(String fileName,
                                     int[] hValues,
                                     int[] rValues,
                                     String[] nLabels,
                                     double[] cUpdate,
                                     double[] dSearch,
                                     double[] eSearch,
                                     double[] fSearch) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> EPSRQ 对齐实验 2: Var H/R/N <<<\n");
            writer.write("Mapping: h->T, l=2^20 -> gamma=2^20(maxFiles)\n");
            writer.write("Datasets: spatial_data_set_10W.csv, 2W~10W.csv\n\n");

            writer.write("Table C: Update Time (Total) vs h\n");
            writer.write(String.format("%-10s | %-12s\n", "h", "EPSRQ(ms)"));
            writer.write("-".repeat(28));
            writer.write("\n");
            for (int i = 0; i < hValues.length; i++) {
                writer.write(String.format("%-10d | %-12.4f\n", hValues[i], cUpdate[i]));
            }
            writer.write("\n");

            writer.write("Table D: Search Time (Avg) vs h\n");
            writer.write(String.format("%-10s | %-12s\n", "h", "EPSRQ(ms)"));
            writer.write("-".repeat(28));
            writer.write("\n");
            for (int i = 0; i < hValues.length; i++) {
                writer.write(String.format("%-10d | %-12.4f\n", hValues[i], dSearch[i]));
            }
            writer.write("\n");

            writer.write("Table E: Search Time (Avg) vs R(%)\n");
            writer.write(String.format("%-10s | %-12s\n", "R(%)", "EPSRQ(ms)"));
            writer.write("-".repeat(28));
            writer.write("\n");
            for (int i = 0; i < rValues.length; i++) {
                writer.write(String.format("%-10d | %-12.4f\n", rValues[i], eSearch[i]));
            }
            writer.write("\n");

            writer.write("Table F: Search Time (Avg) vs N\n");
            writer.write(String.format("%-10s | %-12s\n", "N", "EPSRQ(ms)"));
            writer.write("-".repeat(28));
            writer.write("\n");
            for (int i = 0; i < nLabels.length; i++) {
                writer.write(String.format("%-10s | %-12.4f\n", nLabels[i], fSearch[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
