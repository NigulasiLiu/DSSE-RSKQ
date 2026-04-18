package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * EPSRQ 对齐实验 3（快速对照版）:
 * 与 {@link EPSRQ_Exp_Var_Ow} 相同目标与数据流程，
 * 增加每次 buildIndex 的耗时日志；检索在每个 checkpoint 上采用 warmUp=100、formal=10，
 * 输出文件中 Search(ms) 为 formal 阶段单次查询平均耗时。
 * 结果写入独立文件。
 */
public final class EPSRQ_Exp_Var_Ow_Quick {

    private static final String RESULT_FILE = "experiment_epsrq_3_var_ow_h_12_quick.txt";

    private EPSRQ_Exp_Var_Ow_Quick() {
    }

    public static void main(String[] args) throws Exception {
        String filePath = "src/dataset/spatial_data_set_10W.csv";
        int fixedL = 1 << 20;
        int fixedH = 12;
        int div = 100;
        int searchRangePercent = 3;
        int warmUpTimes = 100;
        int formalTimes = 10;
        int totalSearchLoops = warmUpTimes + formalTimes;
        int[] owCheckpoints = {500, 1000, 1500, 2000, 2500, 3000, 3500};
        int numTestObjects = 20;
        long seed = 20260105L;
        System.out.println("[Init] Start EPSRQ_Exp_Var_Ow_Quick");
        System.out.println("[Init] Loading dataset: " + filePath);
        System.out.printf("[Init] Search loops per checkpoint: warmUp=%d, formal=%d%n", warmUpTimes, formalTimes);
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
                double buildMs = (b1 - b0) / 1e6;
                totalBuildMs[cpIdx] += buildMs;
                System.out.printf("[Timing] Ow buildIndex: %.4f ms (object=%d/%d, checkpoint=%d, rows=%d)%n",
                        buildMs, tObj + 1, numTestObjects, ow, owData.size());

                long searchSumNs = 0L;
                for (int si = 0; si < totalSearchLoops; si++) {
                    int xStart = random.nextInt(Math.max(1, edge - rangeLen));
                    int yStart = random.nextInt(Math.max(1, edge - rangeLen));
                    long s0 = System.nanoTime();
                    epsrq.searchRect(xStart, yStart, rangeLen, target.keywords);
                    long s1 = System.nanoTime();
                    if (si >= warmUpTimes) {
                        searchSumNs += (s1 - s0);
                    }
                }
                totalSearchNs[cpIdx] += searchSumNs;
                doneTasks++;
                if (doneTasks % Math.max(1, owCheckpoints.length) == 0 || doneTasks == totalTasks) {
                    printProgress("Ow(objects x checkpoints)", doneTasks, totalTasks);
                }
            }
        }

        double[] avgSearchMs = new double[owCheckpoints.length];
        double[] avgBuildMs = new double[owCheckpoints.length];
        for (int i = 0; i < owCheckpoints.length; i++) {
            avgSearchMs[i] = (totalSearchNs[i] / numTestObjects) / formalTimes / 1e6;
            avgBuildMs[i] = totalBuildMs[i] / numTestObjects;
        }

        writeResults(RESULT_FILE, owCheckpoints, avgSearchMs, avgBuildMs, warmUpTimes, formalTimes);
        System.out.println("[Done] Results written: " + RESULT_FILE);
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

    private static void writeResults(String fileName,
                                     int[] owCheckpoints,
                                     double[] avgSearchMs,
                                     double[] avgBuildMs,
                                     int warmUpTimes,
                                     int formalTimes) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> EPSRQ 对齐实验 3 (Quick): Search Time vs o_w <<<\n");
            writer.write("Mapping: h->T, l=2^20 -> maxFiles, gamma=1000(fixed)\n");
            writer.write("Dataset: spatial_data_set_10W.csv + o_w history expansion, h=12, R=3%\n");
            writer.write(String.format(
                    "Search: warmUp=%d + formal=%d per checkpoint; Search(ms) 列为 formal 阶段单次查询平均耗时\n\n",
                    warmUpTimes, formalTimes));
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
