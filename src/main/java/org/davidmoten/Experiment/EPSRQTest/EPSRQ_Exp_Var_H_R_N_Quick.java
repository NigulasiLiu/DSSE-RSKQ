package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * EPSRQ 对齐实验 2（快速对照版）:
 * 与 {@link EPSRQ_Exp_Var_H_R_N} 相同逻辑，
 * 额外输出各阶段 buildIndex 耗时，检索为 warmUp=100、formal=10。
 * 结果写入独立文件。
 */
public final class EPSRQ_Exp_Var_H_R_N_Quick {

    private static final String RESULT_FILE = "experiment_epsrq_2_var_h_r_n_quick.txt";

    private EPSRQ_Exp_Var_H_R_N_Quick() {
    }

    public static void main(String[] args) throws Exception {
        String basePath = "src/dataset/spatial_data_set_";
        int fixedL = 1 << 20;
        int div = 100;
        int warmUpTimes = 100;
        int formalTimes = 10;
        int totalLoops = warmUpTimes + formalTimes;
        long seed = 20260105L;
        System.out.println("[Init] Start EPSRQ_Exp_Var_H_R_N_Quick");
        System.out.println("[Init] Loading dataset: " + basePath + "10W.csv");
        System.out.printf("[Init] Search loops: warmUp=%d, formal=%d%n", warmUpTimes, formalTimes);
        Random random = new Random(seed);

        int[] hValues = {8, 10, 12};
        int[] rValues = {1, 5, 10, 15, 20};
        String[] nLabels = {"2W", "4W", "6W", "8W", "10W"};

        double[] cUpdate = new double[hValues.length];
        double[] dSearch = new double[hValues.length];
        double[] eSearch = new double[rValues.length];
        double[] fSearch = new double[nLabels.length];
        int totalTasks = hValues.length + rValues.length + nLabels.length;
        int doneTasks = 0;

        List<FixRangeCompareToConstructionOne.DataRow> raw10W =
                FixRangeCompareToConstructionOne.loadDataFromFile(basePath + "10W.csv");
        System.out.println("[Init] Dataset 10W loaded, rows=" + raw10W.size());

        for (int i = 0; i < hValues.length; i++) {
            int h = hValues[i];
            System.out.printf("[Stage] C&D(h) preparing data: h=%d%n", h);
            List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw10W, h);
            EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, h, fixedL, seed);

            System.out.printf("[Stage] C&D(h) buildIndex begin: h=%d, n=%d%n", h, data.size());
            long upStart = System.nanoTime();
            epsrq.buildIndex(data);
            long upEnd = System.nanoTime();
            System.out.printf("[Stage] C&D(h) buildIndex done: h=%d%n", h);
            cUpdate[i] = (upEnd - upStart) / 1e6;
            System.out.printf("[Timing] C&D(h) buildIndex: %.4f ms (h=%d, n=%d)%n", cUpdate[i], h, data.size());

            dSearch[i] = runSearch(epsrq, data, h, 3, div, random, totalLoops, warmUpTimes, formalTimes);
            doneTasks++;
            printProgress("C&D(h)", doneTasks, totalTasks);
        }

        int fixedH = 10;
        List<FixRangeCompareToConstructionOne.DataRow> fixedData = normalizeData(raw10W, fixedH);
        EPSRQ_Adapter epsrqFixed = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
        System.out.printf("[Stage] E(R) buildIndex begin: h=%d, n=%d%n", fixedH, fixedData.size());
        long eBuildStart = System.nanoTime();
        epsrqFixed.buildIndex(fixedData);
        long eBuildEnd = System.nanoTime();
        System.out.println("[Stage] E(R) buildIndex done");
        System.out.printf("[Timing] E(R) buildIndex: %.4f ms (h=%d, n=%d)%n",
                (eBuildEnd - eBuildStart) / 1e6, fixedH, fixedData.size());
        for (int i = 0; i < rValues.length; i++) {
            eSearch[i] = runSearch(epsrqFixed, fixedData, fixedH, rValues[i], div, random, totalLoops, warmUpTimes, formalTimes);
            doneTasks++;
            printProgress("E(R)", doneTasks, totalTasks);
        }

        for (int i = 0; i < nLabels.length; i++) {
            String label = nLabels[i];
            System.out.println("[Stage] F(N) loading dataset: " + basePath + label + ".csv");
            List<FixRangeCompareToConstructionOne.DataRow> raw =
                    FixRangeCompareToConstructionOne.loadDataFromFile(basePath + label + ".csv");
            List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw, fixedH);
            EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
            System.out.printf("[Stage] F(N) buildIndex begin: N=%s, rows=%d%n", label, data.size());
            long fBuildStart = System.nanoTime();
            epsrq.buildIndex(data);
            long fBuildEnd = System.nanoTime();
            System.out.printf("[Stage] F(N) buildIndex done: N=%s%n", label);
            System.out.printf("[Timing] F(N) buildIndex: %.4f ms (N=%s, rows=%d)%n",
                    (fBuildEnd - fBuildStart) / 1e6, label, data.size());
            fSearch[i] = runSearch(epsrq, data, fixedH, 3, div, random, totalLoops, warmUpTimes, formalTimes);
            doneTasks++;
            printProgress("F(N)", doneTasks, totalTasks);
        }

        writeResults(RESULT_FILE, hValues, rValues, nLabels, cUpdate, dSearch, eSearch, fSearch);
        System.out.println("[Done] Results written: " + RESULT_FILE);
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
            writer.write(">>> EPSRQ 对齐实验 2 (Quick): Var H/R/N <<<\n");
            writer.write("Mapping: h->T, l=2^20 -> maxFiles, gamma=1000(fixed)\n");
            writer.write("Datasets: spatial_data_set_10W.csv, 2W~10W.csv\n");
            writer.write("Search: warmUp=100, formal=10 (Quick 对照版)\n\n");

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

    private static void printProgress(String stage, int current, int total) {
        int percent = (int) ((current * 100.0) / Math.max(1, total));
        System.out.printf("[Progress][%s] %d/%d (%d%%)%n", stage, current, total, percent);
    }
}
