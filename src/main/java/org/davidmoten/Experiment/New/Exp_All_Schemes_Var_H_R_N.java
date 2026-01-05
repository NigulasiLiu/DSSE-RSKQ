package org.davidmoten.Experiment.New;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.Construction.ConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验脚本 2 (V3.0 最终调整版): Exp_All_Schemes_Var_H_R_N
 * * 核心调整:
 * 1. [Cons1 特别处理]: 在实验 C, D, E (基准为 10W 数据) 中，Cons1 仅处理随机抽取的 2^h 条数据。
 * 这意味着 Cons1 的 Update Time 是 2^h 次操作的总耗时，而 RSKQ/SKQ 是 10W 次的总耗时。
 * 2. [基准保持]: RSKQ 和 SKQ 始终处理完整的 10W 数据。
 * 3. [实验 F]: 保持不变，Cons1 需处理完整的 N (2W~10W) 以验证数据量影响。
 * 4. 保留了坐标归一化逻辑。
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
        double[] c_Update_Cons1= new double[hValues.length]; // 注意：这是 2^h 次的时间

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

        System.out.println(">>> 开始实验 2 (V3.0): Cons1 缩减采样 + 坐标归一化");

        // --------------------------------------------------------
        // 1. 实验 C & D: h 变化 (N=10W 背景下)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 C & D: h 变化");
        System.out.println("    RSKQ/SKQ: 处理 10W 数据");
        System.out.println("    Cons1:    处理 2^h 条随机数据 (Setup & Update)");
        System.out.println("============================================================");

        // 加载原始 10W 数据
        List<FixRangeCompareToConstructionOne.DataRow> rawData10W = FixRangeCompareToConstructionOne.loadDataFromFile(basePath + "10W.csv");
        int size10W = rawData10W.size();

        // 计算最大边界用于归一化
        long maxOriginalX = 0, maxOriginalY = 0;
        for(FixRangeCompareToConstructionOne.DataRow row : rawData10W) {
            if(row.pointX > maxOriginalX) maxOriginalX = row.pointX;
            if(row.pointY > maxOriginalY) maxOriginalY = row.pointY;
        }

        for (int i = 0; i < hValues.length; i++) {
            int h = hValues[i];
            System.out.printf(">> 执行中: h=%d ... \n", h);

            // 1. 归一化数据 (10W)
            List<FixRangeCompareToConstructionOne.DataRow> scaledData = normalizeData(rawData10W, h, maxOriginalX, maxOriginalY);

            // ================== RSKQ & SKQ (Full 10W) ==================
            RSKQ_Biginteger rskq = new RSKQ_Biginteger(fixedL, h, 2);
            SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, fixedL, h, 2);

            // RSKQ Update (10W)
            long start = System.nanoTime();
            for (FixRangeCompareToConstructionOne.DataRow row : scaledData) {
                rskq.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            }
            c_Update_RSKQ[i] = (System.nanoTime() - start) / 1e6;

            // SKQ Update (10W)
            start = System.nanoTime();
            for (FixRangeCompareToConstructionOne.DataRow row : scaledData) {
                skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
            }
            c_Update_SKQ[i] = (System.nanoTime() - start) / 1e6;

            // ================== Cons1 (Subset 2^h) ==================
            // 准备 Cons1 数据子集
            List<FixRangeCompareToConstructionOne.DataRow> cons1Data = new ArrayList<>(scaledData);
            Collections.shuffle(cons1Data, random); // 随机打乱
            int subsetSize = Math.min(1 << h, cons1Data.size()); // 取 2^h 条
            cons1Data = cons1Data.subList(0, subsetSize);
            System.out.printf("   [Cons1] 数据集大小缩减为: %d (2^%d)\n", subsetSize, h);

            int[] xCoords = new int[subsetSize];
            int[] yCoords = new int[subsetSize];
            for(int k=0; k<subsetSize; k++) {
                xCoords[k] = (int) cons1Data.get(k).pointX;
                yCoords[k] = (int) cons1Data.get(k).pointY;
            }

            // Cons1 Setup (构建子集索引)
            ConstructionOne cons1 = new ConstructionOne(128, h, subsetSize, xCoords, yCoords);
            cons1.BTx = cons1.buildBinaryTree(h);
            cons1.BTy = cons1.buildBinaryTree(h);
            List<String> idxX = cons1.buildInvertedIndex(h, subsetSize, xCoords);
            List<String> idxY = cons1.buildInvertedIndex(h, subsetSize, yCoords);
            Map<Integer, String> Sx = cons1.buildxNodeInvertedIndex(idxX, h);
            Map<Integer, String> Sy = cons1.buildyNodeInvertedIndex(idxY, h);
            cons1.setupEDS(Sx, Sy);

            // ================== Search Experiment (Exp D) ==================
            // RSKQ/SKQ 在 10W 数据上搜，Cons1 在 2^h 数据上搜
            d_Search_RSKQ[i] = runSearchExperiment(rskq, null, null, scaledData, h, 3, div, random, totalSearchLoops, warmUpTimes);
            d_Search_SKQ[i]  = runSearchExperiment(null, skq, null, scaledData, h, 3, div, random, totalSearchLoops, warmUpTimes);
            d_Search_Cons1[i]= runSearchExperiment(null, null, cons1, cons1Data, h, 3, div, random, totalSearchLoops, warmUpTimes);

            // ================== Cons1 Update Experiment (Exp C) ==================
            // Cons1 Dynamic Update: 仅针对 subsetSize (2^h) 次
            start = System.nanoTime();
            int maxVal = (1 << h) - 1;
            for(int k=0; k<subsetSize; k++) {
                int[] oldP = {xCoords[k], yCoords[k]};
                // 模拟移动
                int newX = Math.min(xCoords[k] + 1, maxVal);
                int newY = Math.min(yCoords[k] + 1, maxVal);
                int[] newP = {newX, newY};

                List<List<String>> updates = cons1.clientUpdate(oldP, newP);
                cons1.serverUpdate(updates);
            }
            c_Update_Cons1[i] = (System.nanoTime() - start) / 1e6;

            System.out.println("   -> 完成.");
            // GC
            rskq = null; skq = null; cons1 = null; scaledData = null; cons1Data = null;
            System.gc();
        }

        // --------------------------------------------------------
        // 2. 实验 E: R 变化 (固定 h=10)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 E: R (Range) 变化 (h=10)");
        System.out.println("    RSKQ/SKQ: 10W 数据");
        System.out.println("    Cons1:    2^10 (1024) 条数据");
        System.out.println("============================================================");

        int fixedH = 10;
        List<FixRangeCompareToConstructionOne.DataRow> scaledDataFixedH = normalizeData(rawData10W, fixedH, maxOriginalX, maxOriginalY);

        // --- RSKQ & SKQ Setup (10W) ---
        System.out.print(">> 初始化 RSKQ/SKQ (10W)... ");
        RSKQ_Biginteger rskqFixed = new RSKQ_Biginteger(fixedL, fixedH, 2);
        SKQ_Biginteger skqFixed = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);
        for(FixRangeCompareToConstructionOne.DataRow row : scaledDataFixedH) {
            rskqFixed.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            skqFixed.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
        }
        System.out.println("完成.");

        // --- Cons1 Setup (Subset 1024) ---
        System.out.print(">> 初始化 Cons1 (1024 subset)... ");
        List<FixRangeCompareToConstructionOne.DataRow> cons1DataFixed = new ArrayList<>(scaledDataFixedH);
        Collections.shuffle(cons1DataFixed, random);
        int subsetSizeFixed = Math.min(1 << fixedH, cons1DataFixed.size()); // 1024
        cons1DataFixed = cons1DataFixed.subList(0, subsetSizeFixed);

        int[] xCFixed = new int[subsetSizeFixed];
        int[] yCFixed = new int[subsetSizeFixed];
        for(int k=0; k<subsetSizeFixed; k++) {
            xCFixed[k] = (int) cons1DataFixed.get(k).pointX;
            yCFixed[k] = (int) cons1DataFixed.get(k).pointY;
        }
        ConstructionOne cons1Fixed = new ConstructionOne(128, fixedH, subsetSizeFixed, xCFixed, yCFixed);
        cons1Fixed.BTx = cons1Fixed.buildBinaryTree(fixedH);
        cons1Fixed.BTy = cons1Fixed.buildBinaryTree(fixedH);
        cons1Fixed.setupEDS(
                cons1Fixed.buildxNodeInvertedIndex(cons1Fixed.buildInvertedIndex(fixedH, subsetSizeFixed, xCFixed), fixedH),
                cons1Fixed.buildyNodeInvertedIndex(cons1Fixed.buildInvertedIndex(fixedH, subsetSizeFixed, yCFixed), fixedH)
        );
        System.out.println("完成.");

        for (int i = 0; i < rValues.length; i++) {
            int r = rValues[i];
            System.out.printf(">> 测试 R=%d%% ... ", r);

            e_Search_RSKQ[i] = runSearchExperiment(rskqFixed, null, null, scaledDataFixedH, fixedH, r, div, random, totalSearchLoops, warmUpTimes);
            e_Search_SKQ[i]  = runSearchExperiment(null, skqFixed, null, scaledDataFixedH, fixedH, r, div, random, totalSearchLoops, warmUpTimes);
            // Cons1 在其子集上搜
            e_Search_Cons1[i]= runSearchExperiment(null, null, cons1Fixed, cons1DataFixed, fixedH, r, div, random, totalSearchLoops, warmUpTimes);

            System.out.println("完成.");
        }

        rskqFixed = null; skqFixed = null; cons1Fixed = null; scaledDataFixedH = null; cons1DataFixed = null;
        System.gc();

        // --------------------------------------------------------
        // 3. 实验 F: N 变化 (固定 h=10, R=3%)
        // --------------------------------------------------------
        System.out.println("\n============================================================");
        System.out.println(">>> 实验 F: N (Dataset) 变化 (h=10, R=3%)");
        System.out.println("    注意: 此处 Cons1 仍处理全量 N 数据 (2W~10W)");
        System.out.println("============================================================");

        for (int i = 0; i < nLabels.length; i++) {
            String label = nLabels[i];
            System.out.printf(">> 加载数据集: %s ... ", label);

            List<FixRangeCompareToConstructionOne.DataRow> currentRawData = FixRangeCompareToConstructionOne.loadDataFromFile(basePath + label + ".csv");
            int currentSize = currentRawData.size();

            // 计算当前数据集边界并归一化
            long currMaxX = 0, currMaxY = 0;
            for(FixRangeCompareToConstructionOne.DataRow row : currentRawData) {
                if(row.pointX > currMaxX) currMaxX = row.pointX;
                if(row.pointY > currMaxY) currMaxY = row.pointY;
            }
            List<FixRangeCompareToConstructionOne.DataRow> currentScaledData = normalizeData(currentRawData, fixedH, currMaxX, currMaxY);

            System.out.printf("共 %d 条. 初始化... ", currentSize);

            // RSKQ & SKQ
            RSKQ_Biginteger rskqN = new RSKQ_Biginteger(fixedL, fixedH, 2);
            SKQ_Biginteger skqN = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);
            for(FixRangeCompareToConstructionOne.DataRow row : currentScaledData) {
                rskqN.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
                skqN.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
            }

            // Cons1 (全量 N)
            int[] xC = new int[currentSize];
            int[] yC = new int[currentSize];
            for(int k=0; k<currentSize; k++) {
                xC[k] = (int) currentScaledData.get(k).pointX;
                yC[k] = (int) currentScaledData.get(k).pointY;
            }
            ConstructionOne cons1N = new ConstructionOne(128, fixedH, currentSize, xC, yC);
            cons1N.BTx = cons1N.buildBinaryTree(fixedH);
            cons1N.BTy = cons1N.buildBinaryTree(fixedH);
            cons1N.setupEDS(
                    cons1N.buildxNodeInvertedIndex(cons1N.buildInvertedIndex(fixedH, currentSize, xC), fixedH),
                    cons1N.buildyNodeInvertedIndex(cons1N.buildInvertedIndex(fixedH, currentSize, yC), fixedH)
            );

            // Search Test
            f_Search_RSKQ[i] = runSearchExperiment(rskqN, null, null, currentScaledData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes);
            f_Search_SKQ[i]  = runSearchExperiment(null, skqN, null, currentScaledData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes);
            f_Search_Cons1[i]= runSearchExperiment(null, null, cons1N, currentScaledData, fixedH, 3, div, random, totalSearchLoops, warmUpTimes);

            System.out.println("完成.");
            rskqN = null; skqN = null; cons1N = null; currentRawData = null; currentScaledData = null;
            System.gc();
        }

        // 输出结果
        String outFileName = "experiment_2_all_schemes.txt";
        writeExperiment2Results(outFileName, hValues, rValues, nLabels,
                c_Update_RSKQ, c_Update_SKQ, c_Update_Cons1,
                d_Search_RSKQ, d_Search_SKQ, d_Search_Cons1,
                e_Search_RSKQ, e_Search_SKQ, e_Search_Cons1,
                f_Search_RSKQ, f_Search_SKQ, f_Search_Cons1);

        System.out.println("\n>>> 所有实验结束，结果已写入: " + outFileName);
    }

    /**
     * 数据归一化
     */
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

    /**
     * 通用 Search 实验运行器
     */
    private static double runSearchExperiment(RSKQ_Biginteger rskq, SKQ_Biginteger skq, ConstructionOne cons1,
                                              List<FixRangeCompareToConstructionOne.DataRow> data,
                                              int h, int rPercent, int div, Random random,
                                              int totalLoops, int warmUp) throws Exception {
        long sumTime = 0;
        int formalCount = totalLoops - warmUp;
        int edgeLength = 1 << h;
        int rangeLen = edgeLength * rPercent / div;
        int dataSize = data.size();

        for (int i = 0; i < totalLoops; i++) {
            int xstart = random.nextInt(Math.max(1, edgeLength - rangeLen));
            int ystart = random.nextInt(Math.max(1, edgeLength - rangeLen));
            FixRangeCompareToConstructionOne.DataRow row = data.get(random.nextInt(dataSize));

            long start = System.nanoTime();
            if (rskq != null) {
                BigInteger[][] matrix = generateHilbertMatrix(rskq.hilbertCurve, xstart, ystart, rangeLen, rangeLen);
                rskq.ObjectSearch(matrix, row.keywords);
            } else if (skq != null) {
                BigInteger[][] matrix = generateHilbertMatrix(skq.hilbertCurve, xstart, ystart, rangeLen, rangeLen);
                skq.Search(matrix, row.keywords);
            } else if (cons1 != null) {
                int[] xRange = {xstart, xstart + rangeLen};
                int[] yRange = {ystart, ystart + rangeLen};
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
            writer.write(">>> 实验 2: RSKQ vs SKQ vs Cons1 对比实验结果 <<<\n");
            writer.write("* 注意: 在 Table 4 和 5 中，Cons1 的 Update 是针对 2^h 条数据，而 RSKQ/SKQ 是针对 10W 条数据。\n\n");

            writer.write("Table 4: Update Time (Total) vs h (Order)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-18s\n", "h", "RSKQ(10W)", "SKQ(10W)", "Cons1(2^h)"));
            writer.write("-".repeat(65) + "\n");
            for (int i=0; i<hVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-18.4f\n", hVals[i], cUpRSKQ[i], cUpSKQ[i], cUpCons1[i]));
            }
            writer.write("\n");

            writer.write("Table 5: Search Time (Avg) vs h (Order)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-18s\n", "h", "RSKQ", "SKQ", "Cons1(Subset)"));
            writer.write("-".repeat(65) + "\n");
            for (int i=0; i<hVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-18.4f\n", hVals[i], dSrchRSKQ[i], dSrchSKQ[i], dSrchCons1[i]));
            }
            writer.write("\n");

            writer.write("Table 6: Search Time (Avg) vs R (Range %)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "R(%)", "RSKQ", "SKQ", "Cons1"));
            writer.write("-".repeat(55) + "\n");
            for (int i=0; i<rVals.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-12.4f\n", rVals[i], eSrchRSKQ[i], eSrchSKQ[i], eSrchCons1[i]));
            }
            writer.write("\n");

            writer.write("Table 7: Search Time (Avg) vs N (Dataset)\n");
            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "N", "RSKQ", "SKQ", "Cons1(Full N)"));
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