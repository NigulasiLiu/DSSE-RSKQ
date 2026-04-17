package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * EPSRQ 对齐实验 3:
 * 对齐 Exp_RSKQ_vs_SKQ_Var_Ow 的实验目标:
 * - 变量 o_w(关键字历史更新次数) 对 Search Time 的影响
 *
 * 说明:
 * - EPSRQ 为静态建库方案，本脚本在每个 o_w 检查点重建一次索引。
 * - 通过为目标对象追加 o_w 条“历史记录”模拟关键词历史增长。
 */
public final class EPSRQ_Exp_Var_Ow {

    private EPSRQ_Exp_Var_Ow() {
    }

    public static void main(String[] args) throws Exception {
        String filePath = "src/dataset/spatial_data_set_10W.csv";
        int fixedL = 1 << 20;
        int fixedH = 12;
        int div = 100;
        int searchRangePercent = 3;
        int[] owCheckpoints = {500, 1000, 1500, 2000, 2500, 3000, 3500};
        int numTestObjects = 20;
        long seed = 20260105L;
        System.out.println("[Init] Start EPSRQ_Exp_Var_Ow");
        System.out.println("[Init] Loading dataset: " + filePath);
        Random random = new Random(seed);

        List<FixRangeCompareToConstructionOne.DataRow> raw =
                FixRangeCompareToConstructionOne.loadDataFromFile(filePath);
        System.out.println("[Init] Raw dataset loaded, rows=" + raw.size());
        List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw, fixedH);
        System.out.println("[Init] Normalized dataset ready, rows=" + data.size() + ", h=" + fixedH);

        double[] totalSearchNs = new double[owCheckpoints.length];
        double[] totalBuildMs = new double[owCheckpoints.length];
        int n = data.size();
        int edge = 1 << fixedH;
        int rangeLen = Math.max(1, edge * searchRangePercent / div);
        int totalTasks = numTestObjects * owCheckpoints.length;
        int doneTasks = 0;

        for (int tObj = 0; tObj < numTestObjects; tObj++) {
            FixRangeCompareToConstructionOne.DataRow target = data.get(random.nextInt(n));
            System.out.printf("[Stage] Ow object %d/%d selected%n", tObj + 1, numTestObjects);
            for (int cpIdx = 0; cpIdx < owCheckpoints.length; cpIdx++) {
                int ow = owCheckpoints[cpIdx];
                List<FixRangeCompareToConstructionOne.DataRow> owData = materializeDataWithHistory(data, target, ow);
                EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed + cpIdx * 131L + tObj);
                System.out.printf("[Stage] Ow buildIndex begin: object=%d/%d, checkpoint=%d, rows=%d%n",
                        tObj + 1, numTestObjects, ow, owData.size());
                long b0 = System.nanoTime();
                epsrq.buildIndex(owData);
                long b1 = System.nanoTime();
                System.out.printf("[Stage] Ow buildIndex done: object=%d/%d, checkpoint=%d%n",
                        tObj + 1, numTestObjects, ow);
                totalBuildMs[cpIdx] += (b1 - b0) / 1e6;

                int xStart = random.nextInt(Math.max(1, edge - rangeLen));
                int yStart = random.nextInt(Math.max(1, edge - rangeLen));
                long s0 = System.nanoTime();
                epsrq.searchRect(xStart, yStart, rangeLen, target.keywords);
                long s1 = System.nanoTime();
                totalSearchNs[cpIdx] += (s1 - s0);
                doneTasks++;
                if (doneTasks % Math.max(1, owCheckpoints.length) == 0 || doneTasks == totalTasks) {
                    printProgress("Ow(objects x checkpoints)", doneTasks, totalTasks);
                }
            }
        }

        double[] avgSearchMs = new double[owCheckpoints.length];
        double[] avgBuildMs = new double[owCheckpoints.length];
        for (int i = 0; i < owCheckpoints.length; i++) {
            avgSearchMs[i] = (totalSearchNs[i] / numTestObjects) / 1e6;
            avgBuildMs[i] = totalBuildMs[i] / numTestObjects;
        }

        writeResults("experiment_epsrq_3_var_ow_h_12.txt", owCheckpoints, avgSearchMs, avgBuildMs);
        System.out.println("[Done] Results written: experiment_epsrq_3_var_ow_h_12.txt");
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

    private static List<FixRangeCompareToConstructionOne.DataRow> materializeDataWithHistory(
            List<FixRangeCompareToConstructionOne.DataRow> base,
            FixRangeCompareToConstructionOne.DataRow target,
            int ow) {
        List<FixRangeCompareToConstructionOne.DataRow> out = new java.util.ArrayList<>(base.size() + ow);
        out.addAll(base);
        for (int i = 0; i < ow; i++) {
            out.add(new FixRangeCompareToConstructionOne.DataRow(
                    target.fileID,
                    target.pointX,
                    target.pointY,
                    target.keywords
            ));
        }
        return out;
    }

    private static void writeResults(String fileName, int[] owCheckpoints, double[] avgSearchMs, double[] avgBuildMs) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> EPSRQ 对齐实验 3: Search Time vs o_w <<<\n");
            writer.write("Mapping: h->T, l=2^20 -> maxFiles, gamma=1000(fixed)\n");
            writer.write("Dataset: spatial_data_set_10W.csv + o_w history expansion, h=12, R=3%\n\n");
            writer.write(String.format("%-10s | %-16s | %-16s\n", "o_w", "BuildIndex(ms)", "Search(ms)"));
            writer.write("-".repeat(50));
            writer.write("\n");
            for (int i = 0; i < owCheckpoints.length; i++) {
                writer.write(String.format("%-10d | %-16.4f | %-16.4f\n", owCheckpoints[i], avgBuildMs[i], avgSearchMs[i]));
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
