package org.davidmoten.Experiment.New;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验脚本 3 (V2.0): Exp_RSKQ_vs_SKQ_Var_Ow
 * * 目标: 对比关键字历史更新次数 (o_w) 对 Search Time 的影响
 * * 方案: RSKQ, SKQ (移除 Cons1)
 * * 变量:
 * - o_w (History Updates): 100, 1000, 5000, 10000, 20000
 * - 固定: N=10W, h=10, R=3%
 * * 逻辑:
 * 1. 初始化 RSKQ 和 SKQ。
 * 2. 选取特定关键字/对象。
 * 3. 循环执行 Add/Del 交替操作，模拟 history accumulation。
 * 4. 在特定的 checkpoint 处执行搜索并计时。
 */
public class Exp_RSKQ_vs_SKQ_Var_Ow {

    public static void main(String[] args) throws Exception {
        // --------------------------------------------------------
        // 全局配置
        // --------------------------------------------------------
        String filepath = "src/dataset/spatial_data_set_10W.csv";
        int fixedL = 1 << 20; // l = 2^20
        int fixedH = 8;      // h = 8，10，12
        int rangePredicate = 1 << 30;
        int div = 100;
        int searchRangePercent = 3; // R = 3%

        // 目标检查点 (Accumulated Updates)
        int[] owCheckpoints = {500, 1000, 1500, 2000, 2500, 3000, 3500};
        int maxOw = owCheckpoints[owCheckpoints.length - 1];

        // 为了统计准确，我们选取多个不同的测试对象取平均
        int numTestObjects = 20;

        Random random = new Random();

        // 结果存储 [Checkpoint Index]
        double[] resRSKQ = new double[owCheckpoints.length];
        double[] resSKQ  = new double[owCheckpoints.length];

        System.out.println(">>> 开始实验 3: Search Time vs o_w (History Updates) [Only RSKQ & SKQ]");
        System.out.printf("    Dataset: 10W, R: 3%%, h: %d\n", fixedH);
        System.out.printf("    Max o_w: %d, Test Objects: %d\n", maxOw, numTestObjects);

        // 1. 加载并归一化数据 (为了匹配 h=10)
        System.out.print(">> 加载数据... ");
        List<FixRangeCompareToConstructionOne.DataRow> rawData = FixRangeCompareToConstructionOne.loadDataFromFile(filepath);
        long maxX = 0, maxY = 0;
        for(FixRangeCompareToConstructionOne.DataRow row : rawData) {
            if(row.pointX > maxX) maxX = row.pointX;
            if(row.pointY > maxY) maxY = row.pointY;
        }
        List<FixRangeCompareToConstructionOne.DataRow> scaledData = normalizeData(rawData, fixedH, maxX, maxY);
        int dataSize = scaledData.size();
        System.out.println("完成.");

        // 2. 初始化 RSKQ / SKQ (Full 10W)
        // 这里的策略是：构建一次完整索引，然后针对选定的测试对象进行额外的 o_w 更新模拟。
        System.out.print(">> 构建初始索引 (10W)... ");
        RSKQ_Biginteger rskq = new RSKQ_Biginteger(fixedL, fixedH, 2);
        SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);

        for(FixRangeCompareToConstructionOne.DataRow row : scaledData) {
            rskq.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
        }
        System.out.println("完成.");

        // 3. 开始测试循环
        System.out.println("\n>> 开始 o_w 模拟测试...");

        // 临时存储每次 checkpoint 的总耗时
        double[] totalTimeRSKQ = new double[owCheckpoints.length];
        double[] totalTimeSKQ  = new double[owCheckpoints.length];

        for (int tObj = 0; tObj < numTestObjects; tObj++) {
            // 随机选择一个目标行进行操作
            FixRangeCompareToConstructionOne.DataRow targetRow = scaledData.get(random.nextInt(dataSize));

            // 当前累计更新次数
            int currentOw = 0;

            for (int cpIdx = 0; cpIdx < owCheckpoints.length; cpIdx++) {
                int targetOw = owCheckpoints[cpIdx];
                int updatesNeeded = targetOw - currentOw;

                // --- 执行更新模拟 (Add/Del Alternating) ---
                for (int u = 0; u < updatesNeeded; u++) {
                    // 模拟: 如果当前是偶数次(从0开始)，则执行Del；奇数次执行Add。
                    // 假设初始状态该对象已存在(构建时Add了一次)，所以第一次额外操作应该是Del。
                    String op = ((currentOw + u) % 2 == 0) ? "del" : "add";

                    // RSKQ Update
                    rskq.ObjectUpdate(new long[]{targetRow.pointX, targetRow.pointY}, targetRow.keywords, new String[]{op}, new int[]{targetRow.fileID});

                    // SKQ Update
                    skq.update(new long[]{targetRow.pointX, targetRow.pointY}, targetRow.keywords, op, new int[]{targetRow.fileID}, rangePredicate);
                }
                currentOw = targetOw;

                // --- 执行 Search 测试并计时 ---
                // 生成查询范围
                int edgeLength = 1 << fixedH;
                int searchRange = edgeLength * searchRangePercent / div;
                int xstart = random.nextInt(Math.max(1, edgeLength - searchRange));
                int ystart = random.nextInt(Math.max(1, edgeLength - searchRange));
                BigInteger[][] matrix = generateHilbertMatrix(rskq.hilbertCurve, xstart, ystart, searchRange, searchRange);

                // RSKQ Search
                long start = System.nanoTime();
                rskq.ObjectSearch(matrix, targetRow.keywords);
                totalTimeRSKQ[cpIdx] += (System.nanoTime() - start);

                // SKQ Search
                start = System.nanoTime();
                skq.Search(matrix, targetRow.keywords);
                totalTimeSKQ[cpIdx] += (System.nanoTime() - start);
            }

            System.out.printf("\r   Test Object %d/%d Done.", tObj + 1, numTestObjects);
        }
        System.out.println();

        // 4. 计算平均值并输出
        for (int i = 0; i < owCheckpoints.length; i++) {
            resRSKQ[i] = (totalTimeRSKQ[i] / numTestObjects) / 1e6; // ms
            resSKQ[i]  = (totalTimeSKQ[i] / numTestObjects) / 1e6;
        }

        // 5. 写入文件
        writeResults("experiment_3_ow_impact_h_8.txt", owCheckpoints, resRSKQ, resSKQ);
        System.out.println("\n>>> 实验结束，结果已写入 experiment_3_ow_impact.txt");
    }

    private static List<FixRangeCompareToConstructionOne.DataRow> normalizeData(List<FixRangeCompareToConstructionOne.DataRow> rawData, int h, long maxX, long maxY) {
        List<FixRangeCompareToConstructionOne.DataRow> scaledData = new ArrayList<>(rawData.size());
        long maxVal = (1L << h) - 1;
        if (maxX == 0) maxX = 1;
        if (maxY == 0) maxY = 1;

        for (FixRangeCompareToConstructionOne.DataRow row : rawData) {
            long newX = (row.pointX * maxVal) / maxX;
            long newY = (row.pointY * maxVal) / maxY;
            scaledData.add(new FixRangeCompareToConstructionOne.DataRow(row.fileID, newX, newY, row.keywords));
        }
        return scaledData;
    }

    private static void writeResults(String fileName, int[] ows, double[] rskq, double[] skq) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> 实验 3: Keyword History (o_w) vs Search Time <<<\n");
            writer.write("Dataset: 10W (Background), R: 3%, h: 10\n\n");

            writer.write(String.format("%-10s | %-12s | %-12s\n", "o_w", "RSKQ(ms)", "SKQ(ms)"));
            writer.write("-".repeat(40) + "\n");

            for (int i = 0; i < ows.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f\n",
                        ows[i], rskq[i], skq[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}