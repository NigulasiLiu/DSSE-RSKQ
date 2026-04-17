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
 * - EPSRQ_Adapter 当前 delete 为 no-op，因此这里用 add/del 交替驱动相同流程，
 *   主要用于与旧实验保持输入/循环结构一致。
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
        Random random = new Random(seed);

        List<FixRangeCompareToConstructionOne.DataRow> raw =
                FixRangeCompareToConstructionOne.loadDataFromFile(filePath);
        List<FixRangeCompareToConstructionOne.DataRow> data = normalizeData(raw, fixedH);

        EPSRQ_Adapter epsrq = new EPSRQ_Adapter(fixedL, fixedH, fixedL, seed);
        for (FixRangeCompareToConstructionOne.DataRow row : data) {
            epsrq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID % fixedL});
        }

        double[] totalSearchNs = new double[owCheckpoints.length];
        int n = data.size();
        int edge = 1 << fixedH;
        int rangeLen = Math.max(1, edge * searchRangePercent / div);

        for (int tObj = 0; tObj < numTestObjects; tObj++) {
            FixRangeCompareToConstructionOne.DataRow target = data.get(random.nextInt(n));
            int currentOw = 0;

            for (int cpIdx = 0; cpIdx < owCheckpoints.length; cpIdx++) {
                int targetOw = owCheckpoints[cpIdx];
                int updatesNeeded = targetOw - currentOw;

                for (int u = 0; u < updatesNeeded; u++) {
                    String op = ((currentOw + u) % 2 == 0) ? "del" : "add";
                    epsrq.update(new long[]{target.pointX, target.pointY}, target.keywords, op, new int[]{target.fileID % fixedL});
                }
                currentOw = targetOw;

                int xStart = random.nextInt(Math.max(1, edge - rangeLen));
                int yStart = random.nextInt(Math.max(1, edge - rangeLen));
                long s0 = System.nanoTime();
                epsrq.searchRect(xStart, yStart, rangeLen, target.keywords);
                long s1 = System.nanoTime();
                totalSearchNs[cpIdx] += (s1 - s0);
            }
        }

        double[] avgSearchMs = new double[owCheckpoints.length];
        for (int i = 0; i < owCheckpoints.length; i++) {
            avgSearchMs[i] = (totalSearchNs[i] / numTestObjects) / 1e6;
        }

        writeResults("experiment_epsrq_3_var_ow_h_12.txt", owCheckpoints, avgSearchMs);
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

    private static void writeResults(String fileName, int[] owCheckpoints, double[] avgSearchMs) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> EPSRQ 对齐实验 3: Search Time vs o_w <<<\n");
            writer.write("Mapping: h->T, l=2^20 -> gamma=2^20(maxFiles)\n");
            writer.write("Dataset: spatial_data_set_10W.csv, h=12, R=3%\n\n");
            writer.write(String.format("%-10s | %-12s\n", "o_w", "EPSRQ(ms)"));
            writer.write("-".repeat(28));
            writer.write("\n");
            for (int i = 0; i < owCheckpoints.length; i++) {
                writer.write(String.format("%-10d | %-12.4f\n", owCheckpoints[i], avgSearchMs[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
