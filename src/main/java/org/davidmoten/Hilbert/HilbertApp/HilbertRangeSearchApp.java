package org.davidmoten.Hilbert.HilbertApp;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;
import org.davidmoten.Hilbert.HilbertComponent.Range;

public class HilbertRangeSearchApp {

    public static void main(String[] args) {
        int order = 7; // 希尔伯特曲线的阶数
        int dimension = 2; // 2D 空间

        // 测试点集
        long[][] points = {
                {0, 0},
                {8, 15},
                {9, 17},
                {64, 64}
        };

        // 搜索范围
        long[] x_axis = {9, 11};   // x 轴范围
        long[] y_axis = {14, 16};  // y 轴范围

        // 将搜索范围转换为希尔伯特值区间
        List<Range> hilbertRanges = calculateHilbertRanges(order, dimension, x_axis, y_axis);

        // 输出每个范围对应的希尔伯特值区间
        System.out.println("搜索区域对应的希尔伯特值区间:");
        for (Range range : hilbertRanges) {
            System.out.println("区间: [" + range.low() + ", " + range.high() + "]");
        }

        // 输出落在搜索范围内的点
        List<long[]> resultPoints = searchPointsInHilbertRanges(points, hilbertRanges);
        System.out.println("搜索范围内的点:");
        for (long[] point : resultPoints) {
            System.out.println("点: [" + point[0] + ", " + point[1] + "]");
        }
    }

    private static List<Range> calculateHilbertRanges(int order, int dimension, long[] x_axis, long[] y_axis) {
        HilbertCurve hilbertCurve = HilbertCurve.bits(order).dimensions(dimension);

        // 定义搜索区域的所有边界点的坐标
        List<long[]> boundaryPoints = new ArrayList<>();
        for (long x = x_axis[0]; x <= x_axis[1]; x++) {
            for (long y = y_axis[0]; y <= y_axis[1]; y++) {
                boundaryPoints.add(new long[]{x, y});
            }
        }

        // 计算每个边界点的希尔伯特值
        List<BigInteger> hilbertIndexes = new ArrayList<>();
        for (long[] point : boundaryPoints) {
            hilbertIndexes.add(hilbertCurve.index(point));
        }

        // 对希尔伯特值排序
        Collections.sort(hilbertIndexes);

        // 合并连续的希尔伯特值，形成区间
        List<Range> hilbertRanges = new ArrayList<>();
        BigInteger start = hilbertIndexes.get(0);
        BigInteger end = start;

        for (int i = 1; i < hilbertIndexes.size(); i++) {
            BigInteger current = hilbertIndexes.get(i);
            if (current.equals(end.add(BigInteger.ONE))) {
                end = current;
            } else {
                hilbertRanges.add(new Range(start.longValue(), end.longValue()));
                start = current;
                end = current;
            }
        }
        hilbertRanges.add(new Range(start.longValue(), end.longValue()));

        return hilbertRanges;
    }

    private static List<long[]> searchPointsInHilbertRanges(long[][] points, List<Range> hilbertRanges) {
        HilbertCurve hilbertCurve = HilbertCurve.bits(7).dimensions(2); // 同样的希尔伯特曲线
        // 存储结果的列表
        List<long[]> resultPoints = new ArrayList<>();

        // 遍历所有点，找到在希尔伯特编码区间内的点
        for (long[] point : points) {
            BigInteger pointHilbertIndex = hilbertCurve.index(point);
            System.out.println("点: [" + point[0] + ", " + point[1] + "] -> 希尔伯特值: " + pointHilbertIndex);

            // 判断该点的希尔伯特值是否落在任何一个希尔伯特值区间内
            for (Range range : hilbertRanges) {
                if (pointHilbertIndex.longValue() >= range.low() && pointHilbertIndex.longValue() <= range.high()) {
                    resultPoints.add(point);
                    break;
                }
            }
        }

        return resultPoints;
    }
}
