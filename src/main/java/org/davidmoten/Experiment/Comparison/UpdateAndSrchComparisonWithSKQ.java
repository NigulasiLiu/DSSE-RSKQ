package org.davidmoten.Experiment.Comparison;

import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;
import org.davidmoten.Scheme.SKQ.SKQ_Biginteger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 实验类：UpdateAndSrchComparisonWithSKQ
 * 目标：对比 RSKQ 和 SKQ 在 10W 数据集下的 Update 和 Search 性能。
 * 改进：将结果输出到 txt 文件，包含详细的性能对比表格。
 */
public class UpdateAndSrchComparisonWithSKQ {

    public static void main(String[] args) throws Exception {
        // 1. 设置文件路径 (仅关注 10W 数据集)
        String filepath = "src/dataset/spatial_data_set_10W.csv";

        // 2. 参数配置
        int rangePredicate = 1 << 30;   // 范围谓词限制
        int[] hilbertOrders = {8, 10, 12}; // Hilbert 曲线阶数 (行变量)
        int div = 100;                  // 划分比例的分母
        int searchRangePercent = 3;     // 搜索范围固定为 3%
        int[] powers = {18, 19, 20, 21, 22, 23, 24}; // maxFiles 配置 (列变量)
        int warmUpTimes = 9000;         // 预热次数
        int formalTimes = 1000;         // 正式测试次数
        int totalSearchLoops = warmUpTimes + formalTimes;

        // 结果存储数组 [OrderIndex][PowerIndex]
        int rows = hilbertOrders.length;
        int cols = powers.length;

        // RSKQ 结果存储
        double[][] rskqUpdateTotalRes = new double[rows][cols];
        double[][] rskqUpdateAvgRes = new double[rows][cols];
        double[][] rskqSearchTotalRes = new double[rows][cols];
        double[][] rskqSearchAvgRes = new double[rows][cols];

        // SKQ 结果存储
        double[][] skqUpdateTotalRes = new double[rows][cols];
        double[][] skqUpdateAvgRes = new double[rows][cols];
        double[][] skqSearchTotalRes = new double[rows][cols];
        double[][] skqSearchAvgRes = new double[rows][cols];

        Random random = new Random();

        System.out.printf("正在加载数据文件：%s ...\n", filepath);
        // 加载数据到内存
        List<FixRangeCompareToConstructionOne.DataRow> dataRows = FixRangeCompareToConstructionOne.loadDataFromFile(filepath);
        int dataSize = dataRows.size();
        System.out.printf("数据加载完成，共 %d 条数据。\n", dataSize);
        System.out.println("==================================================");

        // 3. 遍历 maxFiles (1<<18 到 1<<24) - 列循环
        for (int c = 0; c < cols; c++) {
            int power = powers[c];
            int maxFiles = 1 << power;
            System.out.printf(">>> 当前 MaxFiles 配置: 2^%d (Values: %d) <<<\n", power, maxFiles);

            // 4. 遍历 Hilbert 阶数 - 行循环
            for (int r = 0; r < rows; r++) {
                int hilbertOrder = hilbertOrders[r];
                System.out.printf("开始实验 | Hilbert Order: %d | Dataset: 10W | Search Range: %d%%\n", hilbertOrder, searchRangePercent);

                // 初始化两个方案的实例
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
                    String[] keywords = row.keywords;

                    // --- 测试 RSKQ Update ---
                    // 按照提供的 Code 1 逻辑：add 和 del 两个操作
                    long start = System.nanoTime();
                    rskq.ObjectUpdate(pSet, keywords, new String[]{"add", "del"}, new int[]{row.fileID, (row.fileID + 1) % maxFiles});
                    long end = System.nanoTime();
                    rskqTotalUpdateNano += (end - start);

                    // --- 测试 SKQ Update ---
                    // 按照提供的 Code 1 逻辑：两次分别调用 update
                    start = System.nanoTime();
                    skq.update(pSet, keywords, "add", new int[]{row.fileID}, rangePredicate);
                    skq.update(pSet, keywords, "del", new int[]{(row.fileID + 1) % maxFiles}, rangePredicate);
                    end = System.nanoTime();
                    skqTotalUpdateNano += (end - start);
                }

                // 计算统计结果 (毫秒)
                double rskqTotalUpdateMs = rskqTotalUpdateNano / 1e6;
                double skqTotalUpdateMs = skqTotalUpdateNano / 1e6;
                double rskqAvgUpdateMs = rskqTotalUpdateMs / dataSize; // 注意：这里分母是 dataSize，虽然每次循环做了2次操作(add/del)，但通常视为处理一条数据的平均耗时
                double skqAvgUpdateMs = skqTotalUpdateMs / dataSize;

                // 存储结果
                rskqUpdateTotalRes[r][c] = rskqTotalUpdateMs;
                rskqUpdateAvgRes[r][c] = rskqAvgUpdateMs;
                skqUpdateTotalRes[r][c] = skqTotalUpdateMs;
                skqUpdateAvgRes[r][c] = skqAvgUpdateMs;

                System.out.printf("    Update 完成 (%d 次):\n", dataSize);
                System.out.printf("    [RSKQ] 总耗时: %10.4f ms | 平均耗时: %10.6f ms\n", rskqTotalUpdateMs, rskqAvgUpdateMs);
                System.out.printf("    [SKQ ] 总耗时: %10.4f ms | 平均耗时: %10.6f ms\n", skqTotalUpdateMs, skqAvgUpdateMs);

                // ==========================================
                // Phase 2: Search 性能测试
                // ==========================================
                System.out.println("  [Phase 2] 正在执行 Search 操作 (预热 9000 + 正式 1000)...");

                int edgeLength = 1 << hilbertOrder;
                // 固定搜索范围 3%
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
                    FixRangeCompareToConstructionOne.DataRow row = dataRows.get(random.nextInt(dataSize));
                    String[] queryKw = row.keywords;

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
                double rskqAvgFormalMs = (rskqFormalSearchNano / 1e6) / formalTimes;
                double skqAvgFormalMs = (skqFormalSearchNano / 1e6) / formalTimes;

                // 存储结果
                rskqSearchTotalRes[r][c] = rskqTotalSearch10kMs;
                rskqSearchAvgRes[r][c] = rskqAvgFormalMs;
                skqSearchTotalRes[r][c] = skqTotalSearch10kMs;
                skqSearchAvgRes[r][c] = skqAvgFormalMs;

                System.out.printf("    Search 完成 (共 %d 次):\n", totalSearchLoops);
                System.out.printf("    [RSKQ] 10000次总耗时: %10.4f ms | 正式阶段(1000次)平均耗时: %10.6f ms\n",
                        rskqTotalSearch10kMs, rskqAvgFormalMs);
                System.out.printf("    [SKQ ] 10000次总耗时: %10.4f ms | 正式阶段(1000次)平均耗时: %10.6f ms\n",
                        skqTotalSearch10kMs, skqAvgFormalMs);

                // 清理状态
                rskq.clearSearchTime();
                rskq.clearUpdateTime();
                skq.clearSearchTime();
                skq.clearUpdateTime();
            }
            System.out.println("==================================================");
        }

        // 5. 写入结果到文件
        writeResultsToFile("experiment_results.txt", powers, hilbertOrders,
                rskqUpdateTotalRes, rskqUpdateAvgRes, rskqSearchTotalRes, rskqSearchAvgRes,
                skqUpdateTotalRes, skqUpdateAvgRes, skqSearchTotalRes, skqSearchAvgRes);

        System.out.println("所有实验结束，结果已保存至 experiment_results.txt");
    }

    /**
     * 将所有表格写入文件
     */
    private static void writeResultsToFile(String fileName, int[] powers, int[] orders,
                                           double[][] rskqUpTotal, double[][] rskqUpAvg,
                                           double[][] rskqSearchTotal, double[][] rskqSearchAvg,
                                           double[][] skqUpTotal, double[][] skqUpAvg,
                                           double[][] skqSearchTotal, double[][] skqSearchAvg) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("实验结果汇总\n");
            writer.write("数据集: 10W, Search Range: 3%\n\n");

            // --- RSKQ 表格 ---
            writeSingleTable(writer, "RSKQ - Update Total Time (ms)", powers, orders, rskqUpTotal);
            writeSingleTable(writer, "RSKQ - Update Average Time (ms)", powers, orders, rskqUpAvg);
            writeSingleTable(writer, "RSKQ - Search Total Time (10000 times) (ms)", powers, orders, rskqSearchTotal);
            writeSingleTable(writer, "RSKQ - Search Average Time (Formal) (ms)", powers, orders, rskqSearchAvg);

            writer.write("\n------------------------------------------------------------\n\n");

            // --- SKQ 表格 ---
            writeSingleTable(writer, "SKQ - Update Total Time (ms)", powers, orders, skqUpTotal);
            writeSingleTable(writer, "SKQ - Update Average Time (ms)", powers, orders, skqUpAvg);
            writeSingleTable(writer, "SKQ - Search Total Time (10000 times) (ms)", powers, orders, skqSearchTotal);
            writeSingleTable(writer, "SKQ - Search Average Time (Formal) (ms)", powers, orders, skqSearchAvg);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入单个表格的通用方法
     */
    private static void writeSingleTable(BufferedWriter writer, String title, int[] powers, int[] orders, double[][] data) throws IOException {
        writer.write("Table: " + title + "\n");
        // 写入列头 (Max Files)
        writer.write(String.format("%-10s", "H \\ L"));
        for (int p : powers) {
            writer.write(String.format("| %-12s", "2^" + p));
        }
        writer.write("|\n");

        // 分割线
        writer.write("-".repeat(10 + powers.length * 14) + "\n");

        // 写入每一行 (Hilbert Order)
        for (int i = 0; i < orders.length; i++) {
            writer.write(String.format("%-10d", orders[i]));
            for (int j = 0; j < powers.length; j++) {
                writer.write(String.format("| %-12.4f", data[i][j]));
            }
            writer.write("|\n");
        }
        writer.write("\n");
    }
}