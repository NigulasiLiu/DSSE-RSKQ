package org.davidmoten.Experiment.New;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验脚本 1: Exp_RSKQ_vs_SKQ_Var_L_H_W
 * * 目标: 对比 RSKQ 和 SKQ 方案
 * 数据集: 10W (固定)
 * * 包含实验:
 * 1. 实验 A: 变量 l (MaxFiles) 和 h (HilbertOrder) 变化对 Update 和 Search 性能的影响。
 * 2. 实验 B: 变量 |W_Q| (查询关键字数量) 变化对 Search 性能的影响。
 * * 输出: experiment_1_rskq_vs_skq.txt
 */
public class Exp_RSKQ_vs_SKQ_Var_L_H_W {

    public static void main(String[] args) throws Exception {
        // --------------------------------------------------------
        // 全局配置
        // --------------------------------------------------------
        String filepath = "src/dataset/spatial_data_set_10W.csv";
        int rangePredicate = 1 << 30;
        int div = 100;
        int searchRangePercent = 3; // R = 3%
        int warmUpTimes = 9000;
        int formalTimes = 1000;
        int totalSearchLoops = warmUpTimes + formalTimes;

        System.out.println(">>> [初始化] 正在加载 10W 数据集...");
        List<FixRangeCompareToConstructionOne.DataRow> dataRows = FixRangeCompareToConstructionOne.loadDataFromFile(filepath);
        int dataSize = dataRows.size(); // N = 100,000
        System.out.printf(">>> [初始化] 数据加载完成，共 %d 条。\n\n", dataSize);

        Random random = new Random();

        // --------------------------------------------------------
        // 实验 A: l (MaxFiles) 变, h (HilbertOrder) 变
        // --------------------------------------------------------
        System.out.println("============================================================");
        System.out.println(">>> 实验 A: l (MaxFiles) & h (HilbertOrder) 变化对性能的影响");
        System.out.println("============================================================");

        int[] powers = {18, 19, 20, 21, 22, 23, 24}; // l 变量
        int[] hilbertOrders = {8, 10, 12};           // h 变量

        int rows = hilbertOrders.length;
        int cols = powers.length;

        // 结果存储矩阵 [Order][Power]
        // RSKQ
        double[][] rskqUpTimeTotal = new double[rows][cols];
        double[][] rskqUpTimeAvg   = new double[rows][cols];
        double[][] rskqSrchTimeTotal = new double[rows][cols];
        double[][] rskqSrchTimeAvg   = new double[rows][cols];
        // SKQ
        double[][] skqUpTimeTotal = new double[rows][cols];
        double[][] skqUpTimeAvg   = new double[rows][cols];
        double[][] skqSrchTimeTotal = new double[rows][cols];
        double[][] skqSrchTimeAvg   = new double[rows][cols];

        for (int c = 0; c < cols; c++) {
            int power = powers[c];
            int maxFiles = 1 << power;

            for (int r = 0; r < rows; r++) {
                int h = hilbertOrders[r];
                System.out.printf(">> 执行中: l=2^%d, h=%d ... ", power, h);

                // 1. 初始化
                RSKQ_Biginteger rskq = new RSKQ_Biginteger(maxFiles, h, 2);
                SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, maxFiles, h, 2);

                // 2. Update 阶段 (插入 10W 数据以构建索引)
                long rskqUpStart = System.nanoTime();
                for (FixRangeCompareToConstructionOne.DataRow row : dataRows) {
                    rskq.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add","del"}, new int[]{row.fileID,(row.fileID+1)%maxFiles});
                }
                long rskqUpEnd = System.nanoTime();

                long skqUpStart = System.nanoTime();
                for (FixRangeCompareToConstructionOne.DataRow row : dataRows) {
                    skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
                    skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "del", new int[]{(row.fileID+1)%maxFiles}, rangePredicate);
                }
                long skqUpEnd = System.nanoTime();

                // 记录 Update 结果
                rskqUpTimeTotal[r][c] = (rskqUpEnd - rskqUpStart) / 1e6;
                rskqUpTimeAvg[r][c]   = rskqUpTimeTotal[r][c] / dataSize;
                skqUpTimeTotal[r][c]  = (skqUpEnd - skqUpStart) / 1e6;
                skqUpTimeAvg[r][c]    = skqUpTimeTotal[r][c] / dataSize;

                // 3. Search 阶段
                int edgeLength = 1 << h;
                int rangeLen = edgeLength * searchRangePercent / div;
                // 随机生成查询矩阵 (本次实验固定一个区域进行多次搜索，或者每次随机)
                // 为了统计平均性能，建议每次循环随机生成

                long rskqSrchSum = 0;
                long skqSrchSum = 0;

                for (int i = 0; i < totalSearchLoops; i++) {
                    // 随机查询参数
                    int xstart = random.nextInt(Math.max(1, edgeLength - rangeLen));
                    int ystart = random.nextInt(Math.max(1, edgeLength - rangeLen));
                    BigInteger[][] matrix = generateHilbertMatrix(rskq.hilbertCurve, xstart, ystart, rangeLen, rangeLen);

                    FixRangeCompareToConstructionOne.DataRow row = dataRows.get(random.nextInt(dataSize));
                    String[] queryKw = row.keywords; // |W_Q| = 12

                    // RSKQ Search
                    long start = System.nanoTime();
                    rskq.ObjectSearch(matrix, queryKw);
                    long duration = System.nanoTime() - start;
                    if (i >= warmUpTimes) rskqSrchSum += duration;

                    // SKQ Search
                    start = System.nanoTime();
                    skq.Search(matrix, queryKw);
                    duration = System.nanoTime() - start;
                    if (i >= warmUpTimes) skqSrchSum += duration;
                }

                // 记录 Search 结果 (仅统计正式搜索的 1000 次)
                rskqSrchTimeTotal[r][c] = rskqSrchSum / 1e6;
                rskqSrchTimeAvg[r][c]   = rskqSrchTimeTotal[r][c] / formalTimes;
                skqSrchTimeTotal[r][c]  = skqSrchSum / 1e6;
                skqSrchTimeAvg[r][c]    = skqSrchTimeTotal[r][c] / formalTimes;

                System.out.println("完成.");

                // 清理内存
                rskq = null;
                skq = null;
                System.gc();
            }
        }

        // --------------------------------------------------------
        // 实验 B: |W_Q| (关键字数量) 变
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 B: |W_Q| (关键字数量) 变化对 Search 性能的影响");
        System.out.println("============================================================");

        // 固定参数
        int fixedPower = 20; // 2^20
        int fixedMaxFiles = 1 << fixedPower;
        int fixedH = 10;
        int[] wqCounts = {2, 4, 6, 8, 10, 12};

        double[] rskqWqAvgRes = new double[wqCounts.length];
        double[] skqWqAvgRes = new double[wqCounts.length];

        System.out.printf(">> 初始化固定实例: l=2^%d, h=%d ... ", fixedPower, fixedH);
        RSKQ_Biginteger rskqFixed = new RSKQ_Biginteger(fixedMaxFiles, fixedH, 2);
        SKQ_Biginteger skqFixed = new SKQ_Biginteger(128, rangePredicate, fixedMaxFiles, fixedH, 2);

        // 填充数据
        for (FixRangeCompareToConstructionOne.DataRow row : dataRows) {
            rskqFixed.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            skqFixed.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
        }
        System.out.println("索引构建完成.");

        // 开始测试不同的 |W_Q|
        int edgeLengthB = 1 << fixedH;
        int rangeLenB = edgeLengthB * searchRangePercent / div;

        for (int kIdx = 0; kIdx < wqCounts.length; kIdx++) {
            int k = wqCounts[kIdx];
            System.out.printf(">> 测试 |W_Q| = %d ... ", k);

            long rskqSum = 0;
            long skqSum = 0;

            for (int i = 0; i < totalSearchLoops; i++) {
                int xstart = random.nextInt(Math.max(1, edgeLengthB - rangeLenB));
                int ystart = random.nextInt(Math.max(1, edgeLengthB - rangeLenB));
                BigInteger[][] matrix = generateHilbertMatrix(rskqFixed.hilbertCurve, xstart, ystart, rangeLenB, rangeLenB);

                FixRangeCompareToConstructionOne.DataRow row = dataRows.get(random.nextInt(dataSize));
                // 截取前 k 个关键字
                String[] fullKw = row.keywords;
                String[] queryKw = (fullKw.length >= k) ? Arrays.copyOfRange(fullKw, 0, k) : fullKw;

                long start = System.nanoTime();
                rskqFixed.ObjectSearch(matrix, queryKw);
                long dur = System.nanoTime() - start;
                if (i >= warmUpTimes) rskqSum += dur;

                start = System.nanoTime();
                skqFixed.Search(matrix, queryKw);
                dur = System.nanoTime() - start;
                if (i >= warmUpTimes) skqSum += dur;
            }

            rskqWqAvgRes[kIdx] = (rskqSum / 1e6) / formalTimes;
            skqWqAvgRes[kIdx]  = (skqSum / 1e6) / formalTimes;
            System.out.println("完成.");
        }

        // --------------------------------------------------------
        // 输出结果到文件
        // --------------------------------------------------------
        String outFileName = "experiment_1_rskq_vs_skq.txt";
        writeExperiment1Results(outFileName, powers, hilbertOrders, wqCounts,
                rskqUpTimeTotal, rskqUpTimeAvg, rskqSrchTimeTotal, rskqSrchTimeAvg,
                skqUpTimeTotal, skqUpTimeAvg, skqSrchTimeTotal, skqSrchTimeAvg,
                rskqWqAvgRes, skqWqAvgRes);

        System.out.println("\n>>> 所有实验结束，结果已写入: " + outFileName);
    }

    private static void writeExperiment1Results(String fileName, int[] powers, int[] orders, int[] wqCounts,
                                                double[][] rskqUpTotal, double[][] rskqUpAvg,
                                                double[][] rskqSrchTotal, double[][] rskqSrchAvg,
                                                double[][] skqUpTotal, double[][] skqUpAvg,
                                                double[][] skqSrchTotal, double[][] skqSrchAvg,
                                                double[] rskqWqAvg, double[] skqWqAvg) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> 实验 1: RSKQ vs SKQ 对比实验结果 <<<\n");
            writer.write("Dataset: 10W\n\n");

            // --- 实验 A 输出 ---
            writer.write("=== 实验 A: l (MaxFiles) & h (HilbertOrder) 变化 ===\n\n");

            // 1. Update Total
            writeTable(writer, "RSKQ - Update Total Time (ms)", powers, orders, rskqUpTotal);
            writeTable(writer, "SKQ  - Update Total Time (ms)", powers, orders, skqUpTotal);

            // 2. Update Avg
            writeTable(writer, "RSKQ - Update Avg Time (ms)", powers, orders, rskqUpAvg);
            writeTable(writer, "SKQ  - Update Avg Time (ms)", powers, orders, skqUpAvg);

            // 3. Search Total (1000 times)
            writeTable(writer, "RSKQ - Search Total Time (Formal 1000) (ms)", powers, orders, rskqSrchTotal);
            writeTable(writer, "SKQ  - Search Total Time (Formal 1000) (ms)", powers, orders, skqSrchTotal);

            // 4. Search Avg
            writeTable(writer, "RSKQ - Search Avg Time (ms)", powers, orders, rskqSrchAvg);
            writeTable(writer, "SKQ  - Search Avg Time (ms)", powers, orders, skqSrchAvg);

            // --- 实验 B 输出 ---
            writer.write("\n=== 实验 B: |W_Q| 变化对 Search Time 的影响 (l=2^20, h=10) ===\n\n");
            writer.write(String.format("%-10s", "|W_Q|"));
            for (int k : wqCounts) writer.write(String.format("| %-8d", k));
            writer.write("|\n");
            writer.write("-".repeat(10 + wqCounts.length * 10) + "\n");

            // RSKQ Row
            writer.write(String.format("%-10s", "RSKQ(ms)"));
            for (double val : rskqWqAvg) writer.write(String.format("| %-8.4f", val));
            writer.write("|\n");

            // SKQ Row
            writer.write(String.format("%-10s", "SKQ (ms)"));
            for (double val : skqWqAvg) writer.write(String.format("| %-8.4f", val));
            writer.write("|\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeTable(BufferedWriter writer, String title, int[] powers, int[] orders, double[][] data) throws IOException {
        writer.write("Table: " + title + "\n");
        writer.write(String.format("%-10s", "H \\ L"));
        for (int p : powers) writer.write(String.format("| 2^%-8d", p));
        writer.write("|\n");
        writer.write("-".repeat(10 + powers.length * 11) + "\n");
        for (int i = 0; i < orders.length; i++) {
            writer.write(String.format("%-10d", orders[i]));
            for (int j = 0; j < powers.length; j++) {
                writer.write(String.format("| %-10.4f", data[i][j]));
            }
            writer.write("|\n");
        }
        writer.write("\n");
    }
}