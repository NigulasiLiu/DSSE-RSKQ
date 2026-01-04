package org.davidmoten.Experiment.Comparison;

import org.davidmoten.Scheme.Construction.ConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.*;

import static org.davidmoten.Experiment.TestByUserInput.BRQComparisonInput.generateHilbertMatrix;

/**
 * 这段代码的主要目的：
 * 1. 比较两种不同方案的 GRQ Search 效率：
 * - RSKQ_Biginteger
 * - ConstructionOne
 * <p>
 * 主要实验内容：
 * 1. 加载不同规模的数据集，进行初始化（包括 2W、4W、6W、8W 和 10W 条目）。
 * 2. 预先搜索（预热），测试性能和结果稳定性。
 * 3. 正式搜索，记录两种方案的平均搜索时间。
 * 4. 打印每种方案的性能结果，展示搜索效率对比。
 * <p>
 * 实现过程：
 * - 数据加载与解析：从 CSV 文件加载数据，存储为 DataRow 对象的列表。
 * - 文件中的 Spatial Data 根据二维高斯分布、Zipf 分布随机生成。
 * - 预先搜索：
 * - 随机生成搜索范围，对两种方案进行多次预热查询。
 * - 记录和输出预热阶段的平均查询时间。
 * - 正式搜索：
 * - 在更复杂的查询范围内，比较两种方案的查询效率。
 * - 输出正式搜索阶段的平均查询时间。
 */
public class FixRangeCompareToConstructionOne {

    public static void main(String[] args) throws Exception {
        // 输入文件列表
        String[] filepaths = {
                "src/dataset/spatial_data_set_2W.csv",
                "src/dataset/spatial_data_set_4W.csv",
                "src/dataset/spatial_data_set_6W.csv",
                "src/dataset/spatial_data_set_8W.csv",
                "src/dataset/spatial_data_set_10W.csv"
        };

        // 参数配置
        int rangePredicate = 1 << 30;
        int[] maxfilesArray = {1 << 20};
        int[] hilbertOrders = {8, 10, 12};
        int div = 100;
        int[] searchtimes = {9000, 1000};
        int[] searchtimesForConOne = {100};
        int[] rangeList = {3, 6, 9};
        Random random = new Random();

        // 依次对不同文件进行实验
        for (String filepath : filepaths) {
            System.out.printf("加载数据文件：%s\n", filepath);
            // 加载数据到内存
            List<DataRow> dataRows = loadDataFromFile(filepath);

            // 开始性能评估
            System.out.printf("实例 maxFiles: %d\n", maxfilesArray[0]);
            for (int range : rangeList) {
                System.out.printf("GRQ Range %d%%\n", range);
                for (int hilbertOrder : hilbertOrders) {
                    System.out.printf("正在初始化实例 | hilbertOrder: %d\n", hilbertOrder);
                    RSKQ_Biginteger spqs = new RSKQ_Biginteger(maxfilesArray[0], hilbertOrder, 2);
                    List<Double> consOneSearchTimes = new ArrayList<>();
                    int lambda = 128;
                    int maxCoordinate = 1 << hilbertOrder;
                    int t = (int) (Math.log(maxCoordinate) / Math.log(2));
                    int[] xCoordinates = new int[dataRows.size()];
                    int[] yCoordinates = new int[dataRows.size()];

                    for (int i = 0; i < dataRows.size(); i++) {
                        DataRow row = dataRows.get(i);
                        long[] pSet = new long[]{row.pointX, row.pointY};
                        int[] files = new int[]{row.fileID};
                        spqs.ObjectUpdate(pSet, row.keywords, new String[]{"add"}, files);
                        xCoordinates[i] = Math.toIntExact(row.pointX);
                        yCoordinates[i] = Math.toIntExact(row.pointY);
                    }
                    ConstructionOne con1 = new ConstructionOne(lambda, t, dataRows.size(), xCoordinates, yCoordinates);
                    con1.BTx = con1.buildBinaryTree(t);
                    con1.BTy = con1.buildBinaryTree(t);
                    Map<Integer, String> Sx = con1.buildxNodeInvertedIndex(con1.buildInvertedIndex(t, dataRows.size(), xCoordinates), t);
                    Map<Integer, String> Sy = con1.buildyNodeInvertedIndex(con1.buildInvertedIndex(t, dataRows.size(), yCoordinates), t);
                    con1.setupEDS(Sx, Sy);

                    int edgeLength = 1 << hilbertOrder;
                    // 预先搜索
                    for (int i = 0; i < searchtimes[0]; i++) {
                        int xstart = random.nextInt(edgeLength * (div - 1) / div);
                        int ystart = random.nextInt(edgeLength * (div - 1) / div);
                        int searchRange = edgeLength / div;
                        BigInteger[][] matrixToSearch = generateHilbertMatrix(
                                spqs.hilbertCurve, xstart, ystart, searchRange, searchRange);
                        spqs.GRQSearch(matrixToSearch);
                        if (i > 0 && i % 1000 == 0) {
                            System.out.printf("\n %d次预先搜索完成，平均搜索时间: |RSKQ_Biginteger: |%-10.6f|ms|\n",
                                    i, spqs.getAverageSearchTime());
                            spqs.clearSearchTime();
                        }
                    }

                    // 正式搜索
                    for (int i = 0; i < searchtimes[1]; i++) {
                        int xstart = random.nextInt(edgeLength * (div - range) / div);
                        int ystart = random.nextInt(edgeLength * (div - range) / div);
                        int searchRange = edgeLength * range / div;

                        BigInteger[][] matrixToSearch = generateHilbertMatrix(
                                spqs.hilbertCurve, xstart, ystart, searchRange, searchRange);
                        spqs.GRQSearch(matrixToSearch);
                        if (i % 10 == 0) {
                            int[] rangex = con1.rangeConvert(t, new int[]{xstart, xstart + searchRange});
                            int[] rangey = con1.rangeConvert(t, new int[]{ystart, ystart + searchRange});
                            long startTime = System.nanoTime();
                            con1.clientSearch(rangex, rangey, t);
                            long endTime = System.nanoTime();
                            consOneSearchTimes.add((endTime - startTime) / 1e6);
                        }

                    }

                    // 打印正式搜索时间
                    System.out.printf("\n正式搜索完成，平均搜索时间: |RSKQ_Biginteger(avg of 1000 times): |%-10.6f|ms|| Construction1(avg of 100 times): |%-10.6f|ms|\n",
                            spqs.getAverageSearchTime(),
                            consOneSearchTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
                }
            }
        }
    }

    public static List<DataRow> loadOneData(int maxfiles, int edgelen) throws Exception {
        List<DataRow> dataRows = new ArrayList<>();
        dataRows.add(new DataRow(maxfiles >>1,
                edgelen>>1, edgelen>>1,
                new String[]{
                        "k1", "k1", "k1", "k1",
                        "k1", "k1", "k1", "k1",
                        "k1", "k1", "k1", "k1"}));
        return dataRows;
    }

    public static List<DataRow> loadDataFromFile(String filepath) throws Exception {
        List<DataRow> dataRows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                String[] fields = line.split(",");
                if (fields.length < 15) {
                    System.err.println("Invalid row format: " + line);
                    continue;
                }

                int fileID = Integer.parseInt(fields[0]);
                long pointX = Long.parseLong(fields[1]);
                long pointY = Long.parseLong(fields[2]);
                dataRows.add(new DataRow(fileID, pointX, pointY, new String[]{}));
            }
        }
        return dataRows;
    }

    public static class DataRow {
        public int fileID;
        public long pointX;
        public long pointY;
        public String[] keywords;

        public DataRow(int fileID, long pointX, long pointY, String[] keywords) {
            this.fileID = fileID;
            this.pointX = pointX;
            this.pointY = pointY;
            this.keywords = keywords;
        }
    }
}
