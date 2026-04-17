package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * EPSRQ 对齐实验 1:
 * 对齐 Exp_RSKQ_vs_SKQ_Var_L_H_W 的两类实验:
 * 1) 实验 A: 变量 l/h 对 Update 与 Search 的影响
 * 2) 实验 B: 变量 |W_Q| 对 Search 的影响
 *
 * 参数语义映射:
 * - DSSE 中 h -> EPSRQ 中 T
 * - DSSE 中 l(MaxFiles) -> EPSRQ 中 maxFiles (gamma 在 EPSRQ 固定为 1000)
 *
 * 数据输入对齐:
 * - 统一读取 spatial_data_set_10W.csv
 */
public final class EPSRQ_Exp_Var_L_H_W {

    private EPSRQ_Exp_Var_L_H_W() {
    }

    public static void main(String[] args) throws Exception {
        String filePath = "src/dataset/spatial_data_set_10W.csv";
        int div = 100;
        int searchRangePercent = 3;
        int warmUpTimes = 9000;
        int formalTimes = 1000;
        int totalSearchLoops = warmUpTimes + formalTimes;
        long seed = 20260105L;
        System.out.println("[Init] Start EPSRQ_Exp_Var_L_H_W");
        System.out.println("[Init] Loading dataset: " + filePath);

        List<FixRangeCompareToConstructionOne.DataRow> rawData =
                FixRangeCompareToConstructionOne.loadDataFromFile(filePath);
        System.out.println("[Init] Dataset loaded, rows=" + rawData.size());
        Random random = new Random(seed);

        int[] powers = {18, 19, 20, 21, 22, 23, 24};
        int[] hValues = {8, 10, 12};
        int totalBuildTasks = powers.length * hValues.length;
        int doneBuildTasks = 0;

        double[][] updateTotal = new double[hValues.length][powers.length];
        double[][] updateAvg = new double[hValues.length][powers.length];
        double[][] searchTotal = new double[hValues.length][powers.length];
        double[][] searchAvg = new double[hValues.length][powers.length];

        for (int c = 0; c < powers.length; c++) {
            int power = powers[c];
            int l = 1 << power;

            for (int r = 0; r < hValues.length; r++) {
                int h = hValues[r];
                System.out.printf("[Stage] A(l,h) preparing data: l=2^%d, h=%d%n", power, h);
                List<FixRangeCompareToConstructionOne.DataRow> scaledData = normalizeData(rawData, h);
                int n = scaledData.size();
                int edgeLength = 1 << h;
                int rangeLen = Math.max(1, edgeLength * searchRangePercent / div);

                EPSRQ_Adapter epsrq = new EPSRQ_Adapter(l, h, l, seed);

                System.out.printf("[Stage] A(l,h) buildIndex begin: l=2^%d, h=%d, n=%d%n", power, h, n);
                long upStart = System.nanoTime();
                epsrq.buildIndex(scaledData);
                long upEnd = System.nanoTime();
                System.out.printf("[Stage] A(l,h) buildIndex done: l=2^%d, h=%d%n", power, h);
                updateTotal[r][c] = (upEnd - upStart) / 1e6;
                updateAvg[r][c] = updateTotal[r][c] / n;
                doneBuildTasks++;
                printProgress("Build A(l,h)", doneBuildTasks, totalBuildTasks);

                long searchSum = 0L;
                for (int i = 0; i < totalSearchLoops; i++) {
                    int xStart = random.nextInt(Math.max(1, edgeLength - rangeLen));
                    int yStart = random.nextInt(Math.max(1, edgeLength - rangeLen));
                    FixRangeCompareToConstructionOne.DataRow row = scaledData.get(random.nextInt(n));

                    long s0 = System.nanoTime();
                    epsrq.searchRect(xStart, yStart, rangeLen, row.keywords);
                    long duration = System.nanoTime() - s0;
                    if (i >= warmUpTimes) {
                        searchSum += duration;
                    }
                }
                searchTotal[r][c] = searchSum / 1e6;
                searchAvg[r][c] = searchTotal[r][c] / formalTimes;
            }
        }

        int fixedPower = 20;
        int fixedL = 1 << fixedPower;
        int fixedH = 10;
        int[] wqCounts = {2, 4, 6, 8, 10, 12};
        double[] wqAvgMs = new double[wqCounts.length];

        List<FixRangeCompareToConstructionOne.DataRow> fixedData = normalizeData(rawData, fixedH);
        EPSRQ_Adapter fixedEpsrq = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
        System.out.printf("[Stage] B(|W_Q|) buildIndex begin: h=%d, n=%d%n", fixedH, fixedData.size());
        fixedEpsrq.buildIndex(fixedData);
        System.out.println("[Stage] B(|W_Q|) buildIndex done");

        int fixedEdge = 1 << fixedH;
        int fixedRangeLen = Math.max(1, fixedEdge * searchRangePercent / div);
        int dataSize = fixedData.size();

        for (int i = 0; i < wqCounts.length; i++) {
            int wq = wqCounts[i];
            long searchSum = 0L;
            for (int loop = 0; loop < totalSearchLoops; loop++) {
                int xStart = random.nextInt(Math.max(1, fixedEdge - fixedRangeLen));
                int yStart = random.nextInt(Math.max(1, fixedEdge - fixedRangeLen));
                FixRangeCompareToConstructionOne.DataRow row = fixedData.get(random.nextInt(dataSize));
                String[] queryKeywords = row.keywords.length > wq ? Arrays.copyOfRange(row.keywords, 0, wq) : row.keywords;

                long s0 = System.nanoTime();
                fixedEpsrq.searchRect(xStart, yStart, fixedRangeLen, queryKeywords);
                long duration = System.nanoTime() - s0;
                if (loop >= warmUpTimes) {
                    searchSum += duration;
                }
            }
            wqAvgMs[i] = (searchSum / 1e6) / formalTimes;
            printProgress("Search B(|W_Q|)", i + 1, wqCounts.length);
        }

        writeResults("experiment_epsrq_1_var_l_h_w.txt", powers, hValues, wqCounts,
                updateTotal, updateAvg, searchTotal, searchAvg, wqAvgMs);
        System.out.println("[Done] Results written: experiment_epsrq_1_var_l_h_w.txt");
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
                                     int[] powers,
                                     int[] hValues,
                                     int[] wqCounts,
                                     double[][] updateTotal,
                                     double[][] updateAvg,
                                     double[][] searchTotal,
                                     double[][] searchAvg,
                                     double[] wqAvgMs) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> EPSRQ 对齐实验 1: Var L/H/W <<<\n");
            writer.write("Mapping: h->T, l->maxFiles, gamma=1000(fixed)\n");
            writer.write("Dataset: spatial_data_set_10W.csv\n\n");

            writeTable(writer, "Update Total Time (ms)", powers, hValues, updateTotal);
            writeTable(writer, "Update Avg Time (ms)", powers, hValues, updateAvg);
            writeTable(writer, "Search Total Time (Formal 1000) (ms)", powers, hValues, searchTotal);
            writeTable(writer, "Search Avg Time (ms)", powers, hValues, searchAvg);

            writer.write("=== 实验 B: |W_Q| 变化对 Search Time 的影响 (l=2^20, h=10) ===\n");
            writer.write(String.format("%-10s", "|W_Q|"));
            for (int c : wqCounts) {
                writer.write(String.format("| %-8d", c));
            }
            writer.write("|\n");
            writer.write("-".repeat(10 + wqCounts.length * 10));
            writer.write("\n");
            writer.write(String.format("%-10s", "EPSRQ(ms)"));
            for (double val : wqAvgMs) {
                writer.write(String.format("| %-8.4f", val));
            }
            writer.write("|\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeTable(BufferedWriter writer, String title, int[] powers, int[] hValues, double[][] data)
            throws IOException {
        writer.write("Table: " + title + "\n");
        writer.write(String.format("%-10s", "H \\ L"));
        for (int p : powers) {
            writer.write(String.format("| 2^%-8d", p));
        }
        writer.write("|\n");
        writer.write("-".repeat(10 + powers.length * 11));
        writer.write("\n");
        for (int i = 0; i < hValues.length; i++) {
            writer.write(String.format("%-10d", hValues[i]));
            for (int j = 0; j < powers.length; j++) {
                writer.write(String.format("| %-10.4f", data[i][j]));
            }
            writer.write("|\n");
        }
        writer.write("\n");
    }

    private static void printProgress(String stage, int current, int total) {
        int percent = (int) ((current * 100.0) / Math.max(1, total));
        System.out.printf("[Progress][%s] %d/%d (%d%%)%n", stage, current, total, percent);
    }
}
