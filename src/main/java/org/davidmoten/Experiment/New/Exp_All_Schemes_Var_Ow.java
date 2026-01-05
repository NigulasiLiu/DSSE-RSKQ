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
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验脚本 3: Exp_All_Schemes_Var_Ow
 * * 目标: 对比关键字历史更新次数 (o_w) 对 Search Time 的影响
 * * 方案: RSKQ, SKQ, Cons1
 * * 变量:
 * - o_w (History Updates): 100, 1000, 5000, 10000, 20000
 * - 固定: N=10W (Cons1用子集), h=10, R=3%
 * * 逻辑:
 * 1. 初始化各方案。
 * 2. 选取特定关键字/对象。
 * 3. 循环执行 Add/Del 交替操作，模拟 history accumulation。
 * 4. 在特定的 checkpoint 处执行搜索并计时。
 */
public class Exp_All_Schemes_Var_Ow {

    public static void main(String[] args) throws Exception {
        // --------------------------------------------------------
        // 全局配置
        // --------------------------------------------------------
        String filepath = "src/dataset/spatial_data_set_10W.csv";
        int fixedL = 1 << 20; // l = 2^20
        int fixedH = 10;      // h = 10
        int rangePredicate = 1 << 30;
        int div = 100;
        int searchRangePercent = 3; // R = 3%

        // 目标检查点 (Accumulated Updates)
        int[] owCheckpoints = {100, 1000, 5000, 10000, 20000};
        int maxOw = owCheckpoints[owCheckpoints.length - 1]; // 20000

        // 为了统计准确，我们选取多个不同的测试对象取平均
        int numTestObjects = 20;

        Random random = new Random();

        // 结果存储 [Checkpoint Index]
        double[] resRSKQ = new double[owCheckpoints.length];
        double[] resSKQ  = new double[owCheckpoints.length];
        double[] resCons1= new double[owCheckpoints.length];

        System.out.println(">>> 开始实验 3: Search Time vs o_w (History Updates)");
        System.out.printf("    Dataset: 10W (Cons1 subset 2^%d)\n", fixedH);
        System.out.printf("    Max o_w: %d, Test Objects: %d\n", maxOw, numTestObjects);

        // 1. 加载并归一化数据
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

        // 2. 初始化 Cons1 数据子集 (随机 2^h)
        List<FixRangeCompareToConstructionOne.DataRow> cons1Data = new ArrayList<>(scaledData);
        Collections.shuffle(cons1Data, random);
        int subsetSize = Math.min(1 << fixedH, cons1Data.size());
        cons1Data = cons1Data.subList(0, subsetSize);
        System.out.printf("   [Cons1] Subset Size: %d\n", subsetSize);

        // 3. 循环测试多个对象以取平均值
        // 注意：由于 SKQ 的状态链是不可逆的（Forward Privacy），
        // 我们不能在同一个实例上反复测不同的对象而不受干扰太严重（虽然不同关键字理论上独立）。
        // 但为了严谨，我们在每一轮测试对象时，都重置（或重新初始化）相关实例，
        // 或者更简单：在一个大实例中，选取互不干扰的关键字进行测试。
        // 这里的策略：初始化一次大实例（模拟真实环境），然后随机选 numTestObjects 个关键字，
        // 对每个关键字单独跑 0 -> 20000 的更新流程。

        // --- 初始化 RSKQ / SKQ (Full 10W) ---
        System.out.print(">> 构建初始索引 (RSKQ/SKQ 10W)... ");
        RSKQ_Biginteger rskq = new RSKQ_Biginteger(fixedL, fixedH, 2);
        SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, fixedL, fixedH, 2);
        for(FixRangeCompareToConstructionOne.DataRow row : scaledData) {
            rskq.ObjectUpdate(new long[]{row.pointX, row.pointY}, row.keywords, new String[]{"add"}, new int[]{row.fileID});
            skq.update(new long[]{row.pointX, row.pointY}, row.keywords, "add", new int[]{row.fileID}, rangePredicate);
        }
        System.out.println("完成.");

        // --- 初始化 Cons1 (Subset) ---
        System.out.print(">> 构建初始索引 (Cons1 Subset)... ");
        int[] xC = new int[subsetSize];
        int[] yC = new int[subsetSize];
        for(int k=0; k<subsetSize; k++) {
            xC[k] = (int) cons1Data.get(k).pointX;
            yC[k] = (int) cons1Data.get(k).pointY;
        }
        ConstructionOne cons1 = new ConstructionOne(128, fixedH, subsetSize, xC, yC);
        cons1.BTx = cons1.buildBinaryTree(fixedH);
        cons1.BTy = cons1.buildBinaryTree(fixedH);
        cons1.setupEDS(
                cons1.buildxNodeInvertedIndex(cons1.buildInvertedIndex(fixedH, subsetSize, xC), fixedH),
                cons1.buildyNodeInvertedIndex(cons1.buildInvertedIndex(fixedH, subsetSize, yC), fixedH)
        );
        System.out.println("完成.");

        // 4. 开始测试循环
        System.out.println("\n>> 开始 o_w 模拟测试...");

        // 临时存储每次 checkpoint 的总耗时
        double[] totalTimeRSKQ = new double[owCheckpoints.length];
        double[] totalTimeSKQ  = new double[owCheckpoints.length];
        double[] totalTimeCons1= new double[owCheckpoints.length];

        for (int tObj = 0; tObj < numTestObjects; tObj++) {
            // 随机选择一个目标行进行操作
            FixRangeCompareToConstructionOne.DataRow targetRow = scaledData.get(random.nextInt(dataSize));
            // 确保 Cons1 也有对应的点 (为了 update 模拟)
            int cons1Idx = random.nextInt(subsetSize);
            int[] cons1P = {xC[cons1Idx], yC[cons1Idx]};

            // 当前累计更新次数
            int currentOw = 0;

            // 针对该对象的 Search 预热 (第一次)
            // (略)

            for (int cpIdx = 0; cpIdx < owCheckpoints.length; cpIdx++) {
                int targetOw = owCheckpoints[cpIdx];
                int updatesNeeded = targetOw - currentOw;

                // --- 执行更新模拟 (Add/Del Alternating) ---
                // 我们使用 "add" 然后 "del" 的循环。
                // 这样状态链增长，但实际索引中该元素可能被移除或添加，保持总大小动态平衡。
                // 对 SKQ 而言，del 也是一个 append 操作，所以链长度 = currentOw。

                for (int u = 0; u < updatesNeeded; u++) {
                    String op = ((currentOw + u) % 2 == 0) ? "del" : "add"; // 0:del, 1:add... (假设初始是存在的，所以先del)
                    // 如果初始是add进去的，那么第一次操作应该是del

                    // RSKQ Update
                    rskq.ObjectUpdate(new long[]{targetRow.pointX, targetRow.pointY}, targetRow.keywords, new String[]{op}, new int[]{targetRow.fileID});

                    // SKQ Update
                    skq.update(new long[]{targetRow.pointX, targetRow.pointY}, targetRow.keywords, op, new int[]{targetRow.fileID}, rangePredicate);

                    // Cons1 Update (模拟移动: P -> P' -> P)
                    // 偶数次: P -> P+1; 奇数次: P+1 -> P
                    int maxVal = (1 << fixedH) - 1;
                    int[] p1 = cons1P; // Current
                    int[] p2;          // Next
                    if (op.equals("del")) { // 模拟移动走
                        p2 = new int[]{Math.min(p1[0]+1, maxVal), Math.min(p1[1]+1, maxVal)};
                    } else { // 模拟移回来 ("add")
                        p2 = cons1P; // 回到原点 (这里简化处理，假设 p1 已经是移走后的位置)
                        // 由于 Cons1 的 update 是基于坐标对的，我们其实只需要调用 clientUpdate 产生开销即可
                        // 为了简单，我们总是做 P -> P' 的计算
                        p2 = new int[]{Math.min(p1[0]+1, maxVal), Math.min(p1[1]+1, maxVal)};
                    }
                    cons1.serverUpdate(cons1.clientUpdate(p1, p2));
                }
                currentOw = targetOw;

                // --- 执行 Search 测试并计时 ---
                // 生成查询参数
                int edgeLength = 1 << fixedH;
                int searchRange = edgeLength * searchRangePercent / div;
                // 固定围绕目标点的查询 (或者随机范围，但包含目标关键字)
                // 为了测关键字搜索性能，位置可以是随机的，只要包含该关键字查询
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

                // Cons1 Search (Range Only)
                // Cons1 不受关键字更新影响，但我们记录其耗时作为参考
                start = System.nanoTime();
                int[] xRange = {xstart, xstart + searchRange};
                int[] yRange = {ystart, ystart + searchRange};
                cons1.clientSearch(cons1.rangeConvert(fixedH, xRange), cons1.rangeConvert(fixedH, yRange), fixedH);
                totalTimeCons1[cpIdx] += (System.nanoTime() - start);
            }

            System.out.printf("\r   Test Object %d/%d Done.", tObj + 1, numTestObjects);
        }
        System.out.println();

        // 5. 计算平均值并输出
        for (int i = 0; i < owCheckpoints.length; i++) {
            resRSKQ[i] = (totalTimeRSKQ[i] / numTestObjects) / 1e6; // ms
            resSKQ[i]  = (totalTimeSKQ[i] / numTestObjects) / 1e6;
            resCons1[i]= (totalTimeCons1[i] / numTestObjects) / 1e6;
        }

        // 6. 写入文件
        writeResults("experiment_3_ow_impact.txt", owCheckpoints, resRSKQ, resSKQ, resCons1);
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

    private static void writeResults(String fileName, int[] ows, double[] rskq, double[] skq, double[] cons1) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(">>> 实验 3: Keyword History (o_w) vs Search Time <<<\n");
            writer.write("Dataset: 10W (Background), R: 3%, h: 10\n\n");

            writer.write(String.format("%-10s | %-12s | %-12s | %-12s\n", "o_w", "RSKQ(ms)", "SKQ(ms)", "Cons1(ms)"));
            writer.write("-".repeat(55) + "\n");

            for (int i = 0; i < ows.length; i++) {
                writer.write(String.format("%-10d | %-12.4f | %-12.4f | %-12.4f\n",
                        ows[i], rskq[i], skq[i], cons1[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}