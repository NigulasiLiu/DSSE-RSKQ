package org.davidmoten.Experiment.Comparison;

import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验类：UpdateAndSrchComparisonWithSKQ
 * 目标：对比 RSKQ 和 SKQ 在 10W 数据集下的 Update 和 Search 性能。
 * * 变更记录：
 * 1. 增加 maxFiles 变量控制循环：从 1<<18 到 1<<24。
 * * 实现细节：
 * 1. 数据集：固定为 "src/dataset/spatial_data_set_10W.csv"。
 * 2. Update阶段：统计 10W 次 Update 的总耗时和平均耗时。
 * 3. 变量控制：搜索范围固定为 3%。
 * 4. Search阶段：包含 9000 次预热和 1000 次正式测试，统计 10000 次的总耗时。
 */
public class UpdateAndSrchComparisonWithSKQ {

    public static void main(String[] args) throws Exception {
        // 1. 设置文件路径 (仅关注 10W 数据集)
        String filepath = "src/dataset/spatial_data_set_10W.csv";

        // 2. 参数配置
        int rangePredicate = 1 << 30;   // 范围谓词限制
        int[] hilbertOrders = {8, 10, 12}; // Hilbert 曲线阶数
        int div = 100;                  // 划分比例的分母
        int searchRangePercent = 3;     // 搜索范围固定为 3%
        int[] powers = {18, 19, 20, 21, 22, 23, 24}; // maxFiles 配置
        int warmUpTimes = 9000;         // 预热次数
        int formalTimes = 1000;         // 正式测试次数
        int totalSearchLoops = warmUpTimes + formalTimes;

        Random random = new Random();

        System.out.printf("正在加载数据文件：%s ...\n", filepath);
        // 加载数据到内存
        List<FixRangeCompareToConstructionOne.DataRow> dataRows = FixRangeCompareToConstructionOne.loadDataFromFile(filepath);
        int dataSize = dataRows.size();
        System.out.printf("数据加载完成，共 %d 条数据。\n", dataSize);
        System.out.println("==================================================");

        // 3. 遍历 maxFiles (1<<18 到 1<<24)
        for (int power:powers) {
            int maxFiles = 1 << power;
            System.out.printf(">>> 当前 MaxFiles 配置: 2^%d (Values: %d) <<<\n", power, maxFiles);

            // 4. 遍历 Hilbert 阶数进行实验
            for (int hilbertOrder : hilbertOrders) {
                System.out.printf("开始实验 | Hilbert Order: %d | Dataset: 10W | Search Range: %d%%\n", hilbertOrder, searchRangePercent);

                // 初始化两个方案的实例
                // 注意：RSKQ 和 SKQ 的构造函数需要传入 maxFiles
                RSKQ_Biginteger rskq = new RSKQ_Biginteger(maxFiles, hilbertOrder, 2);
                SKQ_Biginteger skq = new SKQ_Biginteger(128, rangePredicate, maxFiles, hilbertOrder, 2);

                // ==========================================
                // Phase 1: Update 性能测试
                // ==========================================
                System.out.println("  [Phase 1] 正在执行 Update 操作...");

                long rskqTotalUpdateNano = 0;
                long skqTotalUpdateNano = 0;

                // 遍历所有数据行进行插入
                for (FixRangeCompareToConstructionOne.DataRow row : dataRows) {
                    long[] pSet = new long[]{row.pointX, row.pointY};
//                    int[] files = new int[]{row.fileID};
                    String[] keywords = row.keywords;

                    // --- 测试 RSKQ Update ---
                    long start = System.nanoTime();
                    rskq.ObjectUpdate(pSet, keywords, new String[]{"add","del"}, new int[]{row.fileID, (row.fileID+1)%maxFiles});
                    long end = System.nanoTime();
                    rskqTotalUpdateNano += (end - start);

                    // --- 测试 SKQ Update ---
                    start = System.nanoTime();
                    skq.update(pSet, keywords, "add", new int[]{row.fileID}, rangePredicate);
                    skq.update(pSet, keywords, "del", new int[]{(row.fileID+1)%maxFiles}, rangePredicate);
                    end = System.nanoTime();
                    skqTotalUpdateNano += (end - start);
                }

                // 计算统计结果 (毫秒)
                double rskqTotalUpdateMs = rskqTotalUpdateNano / 1e6;
                double skqTotalUpdateMs = skqTotalUpdateNano / 1e6;
                double rskqAvgUpdateMs = rskqTotalUpdateMs / dataSize;
                double skqAvgUpdateMs = skqTotalUpdateMs / dataSize;

                System.out.printf("    Update 完成 (%d 次):\n", dataSize);
                System.out.printf("    [RSKQ] 总耗时: %10.4f ms | 平均耗时: %10.6f ms\n", rskqTotalUpdateMs, rskqAvgUpdateMs);
                System.out.printf("    [SKQ ] 总耗时: %10.4f ms | 平均耗时: %10.6f ms\n", skqTotalUpdateMs, skqAvgUpdateMs);

                // ==========================================
                // Phase 2: Search 性能测试
                // ==========================================
                System.out.println("  [Phase 2] 正在执行 Search 操作 (预热 9000 + 正式 1000)...");

                int edgeLength = 1 << hilbertOrder;
                // 固定搜索范围 3%
                // 随机生成一个查询区域
                int searchRange = edgeLength * searchRangePercent / div;
                // 确保起始点不越界
                int xstart = random.nextInt(Math.max(1, edgeLength - searchRange));
                int ystart = random.nextInt(Math.max(1, edgeLength - searchRange));

                // 生成 Hilbert 矩阵（两个方案共用同一个查询矩阵）
                BigInteger[][] matrixToSearch = generateHilbertMatrix(
                        rskq.hilbertCurve, xstart, ystart, searchRange, searchRange);

                long rskqTotalSearchNano10k = 0; // 10000次的总耗时
                long skqTotalSearchNano10k = 0;  // 10000次的总耗时
                long rskqFormalSearchNano = 0;   // 仅正式1000次的耗时
                long skqFormalSearchNano = 0;    // 仅正式1000次的耗时

                // 执行 10000 次搜索
                for (int i = 0; i < totalSearchLoops; i++) {
                    // 随机选择一个关键字进行搜索
                    FixRangeCompareToConstructionOne.DataRow row = dataRows.get(random.nextInt(dataSize));
                    String[] queryKw = row.keywords; // 使用该行的关键字进行搜索

                    // --- 测试 RSKQ Search ---
                    long start = System.nanoTime();
                    rskq.ObjectSearch(matrixToSearch, queryKw);
                    long end = System.nanoTime();
                    long duration = end - start;
                    rskqTotalSearchNano10k += duration;
                    if (i >= warmUpTimes) {
                        rskqFormalSearchNano += duration;
                    }

                    // --- 测试 SKQ Search ---
                    start = System.nanoTime();
                    skq.Search(matrixToSearch, queryKw);
                    end = System.nanoTime();
                    duration = end - start;
                    skqTotalSearchNano10k += duration;
                    if (i >= warmUpTimes) {
                        skqFormalSearchNano += duration;
                    }
                }

                // 计算 Search 统计结果
                double rskqTotalSearch10kMs = rskqTotalSearchNano10k / 1e6;
                double skqTotalSearch10kMs = skqTotalSearchNano10k / 1e6;

                // 正式测试的平均耗时
                double rskqAvgFormalMs = (rskqFormalSearchNano / 1e6) / formalTimes;
                double skqAvgFormalMs = (skqFormalSearchNano / 1e6) / formalTimes;

                System.out.printf("    Search 完成 (共 %d 次):\n", totalSearchLoops);
                System.out.printf("    [RSKQ] 10000次总耗时: %10.4f ms | 正式阶段(1000次)平均耗时: %10.6f ms\n",
                        rskqTotalSearch10kMs, rskqAvgFormalMs);
                System.out.printf("    [SKQ ] 10000次总耗时: %10.4f ms | 正式阶段(1000次)平均耗时: %10.6f ms\n",
                        skqTotalSearch10kMs, skqAvgFormalMs);

                System.out.println("--------------------------------------------------");

                // 清理状态
                rskq.clearSearchTime();
                rskq.clearUpdateTime();
                skq.clearSearchTime();
                skq.clearUpdateTime();
            }
            System.out.println("==================================================");
        }

        System.out.println("所有实验结束。");
    }
}
