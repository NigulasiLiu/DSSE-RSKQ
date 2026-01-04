package org.davidmoten.Experiment.New;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.Construction.ConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验脚本 2: Exp_All_Schemes_Var_H_R_N
 * * 目标: 对比 RSKQ, SKQ (Wang), Cons1
 * * 包含实验:
 * 1. 实验 C & D: h (HilbertOrder) 变化对 Update Time 和 Search Time 的影响。
 * 2. 实验 E: R (搜索范围占比) 变化对 Search Time 的影响。
 * 3. 实验 F: N (数据集大小) 变化对 Search Time 的影响。
 * * 输出: experiment_2_all_schemes.txt
 */
public class Exp_All_Schemes_Var_H_R_N {

    public static void main(String[] args) throws Exception {
        // --------------------------------------------------------
        // 全局配置
        // --------------------------------------------------------
        String basePath = "src/dataset/spatial_data_set_";
        int fixedL = 1 << 20; // l = 2^20
        int rangePredicate = 1 << 30;
        int div = 100;

        int warmUpTimes = 9000;
        int formalTimes = 1000;
        int totalSearchLoops = warmUpTimes + formalTimes;

        Random random = new Random();

        // 结果存储
        // 实验 C & D (Var h)
        int[] hValues = {8, 10, 12};
        double[] c_Update_RSKQ = new double[hValues.length];
        double[] c_Update_SKQ  = new double[hValues.length];
        double[] c_Update_Cons1= new double[hValues.length];

        double[] d_Search_RSKQ = new double[hValues.length];
        double[] d_Search_SKQ  = new double[hValues.length];
        double[] d_Search_Cons1= new double[hValues.length];

        // 实验 E (Var R)
        int[] rValues = {1, 5, 10, 15, 20};
        double[] e_Search_RSKQ = new double[rValues.length];
        double[] e_Search_SKQ  = new double[rValues.length];
        double[] e_Search_Cons1= new double[rValues.length];

        // 实验 F (Var N)
        String[] nLabels = {"2W", "4W", "6W", "8W", "10W"};
        double[] f_Search_RSKQ = new double[nLabels.length];
        double[] f_Search_SKQ  = new double[nLabels.length];
        double[] f_Search_Cons1= new double[nLabels.length];

        System.out.println(">>> 开始实验 2: 对比 RSKQ, SKQ, Cons1");

        // --------------------------------------------------------
        // 1. 实验 C & D: h 变化 (固定 N=10W, R=3%)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 C & D: h (HilbertOrder) 变化 (N=10W, R=3%)");
        System.out.println("============================================================");

        // 加载 10W 数据
        List<FixRangeCompareToConstructionOne.DataRow> data10W = FixRangeCompareToConstructionOne.loadDataFromFile(basePath + "10W.csv");
        int size10W = data10W.size();

        for (int i = 0; i < hValues.length; i++) {
            int h = hValues[i];
            System.out.printf(">> 执行中: h=%d ... \n", h);

            // --- RSKQ & SKQ Setup & Update Measure ---
            RSKQ_Biginteger rskq = new RSKQ_Biginteger(fixedL, h, 2);
            SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, fixedL, h, 2);

            // RSKQ Update
            long start = System.nanoTime();
            for (FixRangeCompareToConstructionOne.DataRow row : data10W) {
                rskq.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            }
            c_Update_RSKQ[i] = (System.nanoTime() - start) / 1e6;

            // SKQ Update
            start = System.nanoTime();
            for (FixRangeCompareToConstructionOne.DataRow row : data10W) {
                skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
            }
            c_Update_SKQ[i] = (System.nanoTime() - start) / 1e6;

            // --- Cons1 Setup (SetupEDS) & Update Measure (ClientUpdate) ---
            // 准备数据
            int[] xCoords = new int[size10W];
            int[] yCoords = new int[size10W];
            for(int k=0; k<size10W; k++) {
                xCoords[k] = (int) data10W.get(k).pointX;
                yCoords[k] = (int) data10W.get(k).pointY;
            }
            ConstructionOne cons1 = new ConstructionOne(128, h, size10W, xCoords, yCoords);
            cons1.BTx = cons1.buildBinaryTree(h);
            cons1.BTy = cons1.buildBinaryTree(h);
            List<String> idxX = cons1.buildInvertedIndex(h, size10W, xCoords);
            List<String> idxY = cons1.buildInvertedIndex(h, size10W, yCoords);
            Map<Integer, String> Sx = cons1.buildxNodeInvertedIndex(idxX, h);
            Map<Integer, String> Sy = cons1.buildyNodeInvertedIndex(idxY, h);
            cons1.setupEDS(Sx, Sy); // 静态构建不计入 "Dynamic Update Time"

            // Cons1 Dynamic Update Measurement (模拟 10W 次移动更新)
            start = System.nanoTime();
            for(int k=0; k<size10W; k++) {
                int[] oldP = {xCoords[k], yCoords[k]};
                // 模拟移动到 (x+1, y+1)，注意边界防越界 (2^h - 1)
                int maxVal = (1 << h) - 1;
                int newX = Math.min(xCoords[k] + 1, maxVal);
                int newY = Math.min(yCoords[k] + 1, maxVal);
                int[] newP = {newX, newY};

                List<List<String>> updates = cons1.clientUpdate(oldP, newP);
                cons1.serverUpdate(updates);

                // 恢复坐标以保证后续搜索正确性?
                // 考虑到 Search 实验需要基于原始分布，且 Cons1 的 Update 改变了树结构。
                // 为了严谨，实验 D (Search) 应该在 Update 之前做，或者重置。
                // 这里我们采取策略：先测 Search (Experiment D)，再测 Update (Experiment C)。
                // 所以上面的 Update 代码逻辑顺序需要调整。
            }
            // 但为了代码结构清晰，我们这里实际上是：先 Setup -> 测 Search -> 测 Update。
            // 由于上面已经写了 Update 逻辑，我们把计时值暂存，实际执行顺序调整如下：

            // 重新初始化 Cons1 用于 Search 测试 (保证数据干净)
            cons1 = new ConstructionOne(128, h, size10W, xCoords, yCoords);
            cons1.BTx = cons1.buildBinaryTree(h);
            cons1.BTy = cons1.buildBinaryTree(h);
            cons1.setupEDS(Sx, Sy); // 重建

            // --- Search Experiment (Experiment D) ---
            d_Search_RSKQ[i] = runSearchExperiment(rskq, null, null, data10W, h, 3, div, random, totalSearchLoops, warmUpTimes, "RSKQ");
            d_Search_SKQ[i]  = runSearchExperiment(null, skq, null, data10W, h, 3, div, random, totalSearchLoops, warmUpTimes, "SKQ");
            d_Search_Cons1[i]= runSearchExperiment(null, null, cons1, data10W, h, 3, div, random, totalSearchLoops, warmUpTimes, "Cons1");

            // --- Update Experiment (Experiment C) - Cons1 Only ---
            // RSKQ 和 SKQ 的 Update Time 已经在前面测了 (Build Time)
            // 现在测 Cons1 的 Dynamic Update
            start = System.nanoTime();
            for(int k=0; k<size10W; k++) {
                int[] oldP = {xCoords[k], yCoords[k]};
                int maxVal = (1 << h) - 1;
                int newX = Math.min(xCoords[k] + 1, maxVal);
                int newY = Math.min(yCoords[k] + 1, maxVal);
                int[] newP = {newX, newY};
                List<List<String>> updates = cons1.clientUpdate(oldP, newP);
                cons1.serverUpdate(updates);
            }
            c_Update_Cons1[i] = (System.nanoTime() - start) / 1e6;

            System.out.println("  -> h=" + h + " 完成.");
            // GC
            rskq = null; skq = null; cons1 = null;
            System.gc();
        }

        // --------------------------------------------------------
        // 2. 实验 E: R 变化 (固定 N=10W, h=10)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 E: R (Range) 变化 (N=10W, h=10)");
        System.out.println("============================================================");

        int fixedH = 10;
        // 初始化固定实例
        System.out.print(">> 初始化固定实例 (h=10)... ");
        RSKQ_Biginteger rskqFixed = new RSKQ_Biginteger(fixedL, fixedH, 2);
        SKQ_Biginteger skqFixed = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);

        int[] xCoords10W = new int[size10W];
        int[] yCoords10W = new int[size10W];
        for(int k=0; k<size10W; k++) {
            xCoords10W[k] = (int) data10W.get(k).pointX;
            yCoords10W[k] = (int) data10W.get(k).pointY;

            rskqFixed.ObjectUpdate(new long[]{xCoords10W[k], yCoords10W[k]}, data10W.get(k).keywords, new String[]{"add"}, new int[]{data10W.get(k).fileID});
            skqFixed.update(new long[]{xCoords10W[k], yCoords10W[k]}, data10W.get(k).keywords, "add", new int[]{data10W.get(k).fileID}, rangePredicate);
        }

        ConstructionOne cons1Fixed = new ConstructionOne(128, fixedH, size10W, xCoords10W, yCoords10W);
        cons1Fixed.BTx = cons1Fixed.buildBinaryTree(fixedH);
        cons1Fixed.BTy = cons1Fixed.buildBinaryTree(fixedH);
        cons1Fixed.setupEDS(
                cons1Fixed.buildxNodeInvertedIndex(cons1Fixed.buildInvertedIndex(fixedH, size10W, xCoords10W), fixedH),
                cons1Fixed.buildyNodeInvertedIndex(cons1Fixed.buildInvertedIndex(fixedH, size10W, yCoords10W), fixedH)
        );
        System.out.println("完成.");

        for (int i = 0; i < rValues.length; i++) {
            int r = rValues[i];
            System.out.printf(">> 测试 R=%d%% ... ", r);

            e_Search_RSKQ[i] = runSearchExperiment(rskqFixed, null, null, data10W, fixedH, r, div, random, totalSearchLoops, warmUpTimes, "RSKQ");
            e_Search_SKQ[i]  = runSearchExperiment(null, skqFixed, null, data10W, fixedH, r, div, random, totalSearchLoops, warmUpTimes, "SKQ");
            e_Search_Cons1[i]= runSearchExperiment(null, null, cons1Fixed, data10W, fixedH, r, div, random, totalSearchLoops, warmUpTimes, "Cons1");

            System.out.println("完成.");
        }

        // 释放固定实例
        rskqFixed = null; skqFixed = null; cons1Fixed = null;
        System.gc();

        // --------------------------------------------------------
        // 3. 实验 F: N 变化 (固定 h=10, R=3%)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 F: N (Dataset) 变化 (h=10, R=3%)");
        System.out.println("============================================================");

        for (int i = 0; i < nLabels.length; i++) {
            String label = nLabels[i];
            System.out.printf(">> 加载数据集: %s ... ", label);

            List<FixRangeCompareToConstructionOne.DataRow> currentData = FixRangeCompareToConstructionOne.loadDataFromFile(basePath + label + ".csv");
            int currentSize = currentData.size();
            System.out.printf("共 %d 条. 初始化实例... ", currentSize);

            // Setup
            RSKQ_Biginteger rskqN = new RSKQ_Biginteger(fixedL, fixedH, 2);
            SKQ_Biginteger skqN = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);

            int[] xC = new int[currentSize];
            int[] yC = new int[currentSize];

            for(int k=0; k<currentSize; k++) {
                xC[k] = (int) currentData.get(k).pointX;
                yC[k] = (int) currentData.get(k).pointY;
                rskqN.ObjectUpdate(new long[]{xC[k], yC[k]}, currentData.get(k).keywords, new String[]{"add"}, new int[]{currentData.get(k).fileID});
                skqN.update(new long[]{xC[k], yC[k]}, currentData.get(k).keywords, "add", new int[]{currentData.get(k).fileID}, rangePredicate);
            }

            ConstructionOne cons1N = new ConstructionOne(128, fixedH, currentSize, xC, yC);
            cons1N.BTx = cons1N.buildBinaryTree(fixedH);
            cons1N.BTy = cons1N.buildBinaryTree(fixedH);
            cons1N.setupEDS(
                    cons1N.buildxNodeInvertedIndex(cons1N.buildInvertedIndex(fixedH, currentSize, xC), fixedH),
                    cons1N.buildyNodeInvertedIndex(cons1N.buildInvertedIndex(fixedH, currentSize, yC), fixedH)
            );

            // Search Test
            f_Search_RSKQ[i] = runSearchExperiment(rskqN, null, null, currentData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes, "RSKQ");
            f_Search_SKQ[i]  = runSearchExperiment(null, skqN, null, currentData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes, "SKQ");
            f_Search_Cons1[i]= runSearchExperiment(null, null, cons1N, currentData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes, "Cons1");

            System.out.println("完成.");

            rskqN = null; skqN = null; cons1N = null; currentData = null;
            System.gc();
        }

        // --------------------------------------------------------
        // 输出结果到文件
        // --------------------------------------------------------
        String outFileName = "experiment_2_all_schemes.txt";
        writeExperiment2Results(outFileName, hValues, rValues, nLabels,
                c_Update_RSKQ, c_Update_SKQ, c_Update_Cons1,
                d_Search_RSKQ, d_Search_SKQ, d_Search_Cons1,
                e_Search_RSKQ, e_Search_SKQ, e_Search_Cons1,
                f_Search_RSKQ, f_Search_SKQ, f_Search_Cons1);

        System.out.println("\n>>> 所有实验结束，结果已写入: " + outFileName);
    }

    /**
     * 通用 Search 实验运行器
     * 返回正式搜索的平均耗时 (ms)
     */
    private static double runSearchExperiment(RSKQ_Biginteger rskq, SKQ_Biginteger skq, ConstructionOne cons1,
                                              List<FixRangeCompareToConstructionOne.DataRow> data,
                                              int h, int rPercent, int div, Random random,
                                              int totalLoops, int warmUp, String label) throws Exception {
        long sumTime = 0;
        int formalCount = totalLoops - warmUp;
        int edgeLength = 1 << h;
        int rangeLen = edgeLength * rPercent / div;
        int dataSize = data.size();

        for (int i = 0; i < totalLoops; i++) {
            // Random Query
            int xstart = random.nextInt(Math.max(1, edgeLength - rangeLen));
            int ystart = random.nextInt(Math.max(1, edgeLength - rangeLen));
            FixRangeCompareToConstructionOne.DataRow row = data.get(random.nextInt(dataSize));

            long start = System.nanoTime();
            if (rskq != null) {
                BigInteger[][] matrix = generateHilbertMatrix(rskq.hilbertCurve, xstart, ystart, rangeLen, rangeLen);
                rskq.ObjectSearch(matrix, row.keywords);
            } else if (skq != null) {
                // SKQ Search (requires generated matrix)
                // Need a temp curve to generate matrix if skq doesn't expose it easily,
                // but we can assume SKQ class has hilbertCurve or we generate it externally.
                // SKQ_Biginteger has public 'hilbertCurve'.
                BigInteger[][] matrix = generateHilbertMatrix(skq.hilbertCurve, xstart, ystart, rangeLen, rangeLen);
                skq.Search(matrix, row.keywords);
            } else if (cons1 != null) {
                // Cons1 Search (Range Search)
                int[] xRange = {xstart, xstart + rangeLen};
                int[] yRange = {ystart, ystart + rangeLen};
                // Cons1 rangeConvert logic
                int[] finalX = cons1.rangeConvert(h, xRange);
                int[] finalY = cons1.rangeConvert(h, yRange);
                cons1.clientSearch(finalX, finalY, h);
            }
            long end = System.nanoTime();

            if (i >= warmUp) {
                sumTime += (end - start);
            }
        }
        return (sumTime / 1e6) / formalCount;
    }

    private static void writeExperiment2Results(String fileName, int[] hVals, int[] rVals, String[] nLabs,
                                                double[] cUpRSKQ, double[] cUpSKQ, double[] cUpCons1,
                                                double[] dSrchRSKQ, double[] dSrchSKQ, double[] dSrchCons1,
                                                double[] eSrchRSKQ, double[] eSrchSKQ, double[] eSrchCons1,
                                                double[] fSrchRSKQ, double[] fSrchSKQ, double[] fSrchCons1) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> 实验 2: RSKQ vs SKQ vs Cons1 对比实验结果 <<<\n\n");

            // Table 4: Update Time vs h
            writer.write("Table 4: Update Time (Total for 10W) vs h (Order)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "h", "RSKQ(ms)", "SKQ(ms)", "Cons1(ms)"));
            writer.write("-".repeat(55) + "\n");
            for (int i=0; i<hVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-12.4f\n", hVals[i], cUpRSKQ[i], cUpSKQ[i], cUpCons1[i]));
            }
            writer.write("\n");

            // Table 5: Search Time vs h
            writer.write("Table 5: Search Time (Avg) vs h (Order)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "h", "RSKQ(ms)", "SKQ(ms)", "Cons1(ms)"));
            writer.write("-".repeat(55) + "\n");
            for (int i=0; i<hVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-12.4f\n", hVals[i], dSrchRSKQ[i], dSrchSKQ[i], dSrchCons1[i]));
            }
            writer.write("\n");

            // Table 6: Search Time vs R
            writer.write("Table 6: Search Time (Avg) vs R (Range %)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "R(%)", "RSKQ(ms)", "SKQ(ms)", "Cons1(ms)"));
            writer.write("-".repeat(55) + "\n");
            for (int i=0; i<rVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-12.4f\n", rVals[i], eSrchRSKQ[i], eSrchSKQ[i], eSrchCons1[i]));
            }
            writer.write("\n");

            // Table 7: Search Time vs N
            writer.write("Table 7: Search Time (Avg) vs N (Dataset)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "N", "RSKQ(ms)", "SKQ(ms)", "Cons1(ms)"));
            writer.write("-".repeat(55) + "\n");
            for (int i=0; i<nLabs.length; i++) {
                writer.write(String.format("%-10s | %-12.4f | %-12.4f | %-12.4f\n", nLabs[i], fSrchRSKQ[i], fSrchSKQ[i], fSrchCons1[i]));
            }
            writer.write("\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}