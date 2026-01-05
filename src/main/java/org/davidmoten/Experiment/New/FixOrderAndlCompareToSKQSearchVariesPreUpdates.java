package org.davidmoten.Experiment.New;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;
import org.davidmoten.Scheme.RSKQ.RSKQ_Biginteger;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne.loadOneData;

public class FixOrderAndlCompareToSKQSearchVariesPreUpdates {

    public static void main(String[] args) throws Exception {

        // 参数配置
        int[] maxfilesArray = {1 << 20}; // 最大文件数
        int[] hilbertOrders = {12}; // Hilbert 曲线阶数
        int[] keyNums = {4, 8, 12}; // Hilbert 曲线阶数
        int div = 100; // 划分搜索区域比例
//        int[] updateTimes = {40, 80, 120, 160, 200}; // 插入次数
        int[] updateTimes = {500, 750,1000, 1250,1500, 1750,2000, 2250,2500}; // 插入次数
//        int[] searchtimes = {900,100}; // 预先搜索和正式搜索次数
        // 开始性能评估
        System.out.printf("实例 maxFiles: %d\n", maxfilesArray[0]);
        for (int updateTime : updateTimes) {
            for (int keyNum : keyNums) {
                System.out.printf("keyNum: %d\n", keyNum);
                for (int hilbertOrder : hilbertOrders) {
                    List<FixRangeCompareToConstructionOne.DataRow> dataRows = loadOneData(maxfilesArray[0], 1 << hilbertOrder);
                    System.out.printf("正在初始化实例 | hilbertOrder: %d\n", hilbertOrder);
                    RSKQ_Biginteger spqs = new RSKQ_Biginteger(maxfilesArray[0], hilbertOrder, 2);
                    // 执行更新操作
                    long[] pSet = new long[]{dataRows.get(0).pointX, dataRows.get(0).pointY};
                    int[] files = new int[]{dataRows.get(0).fileID};
                    String[] keywords_1 = dataRows.get(0).keywords;
                    for (int i = 0; i < updateTime; i++) {
                        spqs.ObjectUpdate(pSet, Arrays.copyOfRange(keywords_1, 0, Math.min(keyNum, keywords_1.length)), new String[]{"add"}, new int[]{(files[0] + i) % maxfilesArray[0]});
                    }
                    System.out.printf("插入%d个数据完成；", updateTime);
                    int edgeLength = 1 << hilbertOrder; // Hilbert 边长
                    // 预先搜索
                    int xstart = (int) pSet[0];
                    int ystart = (int) pSet[1];
                    int searchRange = 1;

//                    BigInteger[][] matrixToSearch = generateHilbertMatrix(
//                            spqs.hilbertCurve, xstart, ystart, searchRange, searchRange);
                    BigInteger[][] matrixToSearch = new BigInteger[][]{{spqs.hilbertCurve.index(xstart, ystart)}};
                    spqs.ObjectSearch(matrixToSearch, Arrays.copyOfRange(keywords_1, 0, Math.min(keyNum, keywords_1.length)));
                    System.out.printf("\n搜索完成，平均搜索时间: |RSKQ_Biginteger: |%-10.6f|ms|\n",
                            spqs.getAverageSearchTime());

                    spqs.clearSearchTime();

                }
            }
        }
    }
}
