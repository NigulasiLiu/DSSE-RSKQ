package org.davidmoten.Hilbert.HilbertComponent;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class GetPointIndex {

    public static void main(String[] args) {
        int order = 17; // 希尔伯特曲线的阶数
        int dimension = 2; // 2D 空间
        int maxDimension = (int) Math.pow(2, order); // 2^order
        int maxDistance = 3000; // 矩形的最大宽度和高度
        int k = 1000; // 预设的小正方形数量

        // 创建 HilbertCurve 实例
        HilbertCurve hilbertCurve = HilbertCurve.bits(order).dimensions(dimension);

        // 随机生成两个点
        Random random = new Random();
        long x1 = random.nextInt(maxDimension); // 随机生成x坐标
        long y1 = random.nextInt(maxDimension); // 随机生成y坐标

        // 确保第二个点与第一个点的距离小于 maxDistance
        long x2 = x1 + random.nextInt(Math.min(maxDistance, maxDimension - (int) x1));
        long y2 = y1 + random.nextInt(Math.min(maxDistance, maxDimension - (int) y1));

        // 确定矩形的左上角和右下角
        long leftX = Math.min(x1, x2);
        long rightX = Math.max(x1, x2);
        long topY = Math.min(y1, y2);
        long bottomY = Math.max(y1, y2);

        // 计算当前矩形区域的总点数
        long totalPoints = (rightX - leftX + 1) * (bottomY - topY + 1);

        // 根据总点数和预设值 k 计算实际的矩形区域大小
        double scaleFactor = Math.sqrt((double) k / totalPoints);
        long scaledWidth = (long) Math.ceil((rightX - leftX + 1) * scaleFactor);
        long scaledHeight = (long) Math.ceil((bottomY - topY + 1) * scaleFactor);

        // 确保缩放后的矩形区域大小不超过原始区域
        scaledWidth = Math.min(scaledWidth, rightX - leftX + 1);
        scaledHeight = Math.min(scaledHeight, bottomY - topY + 1);

        // 打印缩放后的矩形区域大小
        System.out.println("缩放后的矩形区域宽度: " + scaledWidth);
        System.out.println("缩放后的矩形区域高度: " + scaledHeight);

        // 重新确定矩形的右下角位置
        long newRightX = Math.min(leftX + scaledWidth - 1, rightX);
        long newBottomY = Math.min(topY + scaledHeight - 1, bottomY);

        System.out.println("缩放后的矩形左上角: (" + leftX + ", " + topY + ")");
        System.out.println("缩放后的矩形右下角: (" + newRightX + ", " + newBottomY + ")");

        // 存储矩形区域内的所有点及其 Hilbert 值
        List<PointHilbert> pointsWithHilbertValues = new ArrayList<>();

        // 遍历矩形区域内的所有点
        for (long x = leftX; x <= newRightX; x++) {
            for (long y = topY; y <= newBottomY; y++) {
                long[] point = {x, y};
                BigInteger hilbertValue = hilbertCurve.index(point);
                pointsWithHilbertValues.add(new PointHilbert(point, hilbertValue));
            }
        }

        // 按照 Hilbert 值对点进行排序
        Collections.sort(pointsWithHilbertValues);

        // 打印二维坐标和对应的 Hilbert 值
        System.out.println("点坐标和对应的 Hilbert 曲线值:");
        printPointsWithHilbertValues(pointsWithHilbertValues);
    }

    // 定义一个类用于存储点及其 Hilbert 值
    static class PointHilbert implements Comparable<PointHilbert> {
        long[] point;
        BigInteger hilbertValue;

        public PointHilbert(long[] point, BigInteger hilbertValue) {
            this.point = point;
            this.hilbertValue = hilbertValue;
        }

        @Override
        public int compareTo(PointHilbert other) {
            return this.hilbertValue.compareTo(other.hilbertValue);
        }
    }

    // 打印点坐标和对应的 Hilbert 值
    static void printPointsWithHilbertValues(List<PointHilbert> pointsWithHilbertValues) {
        for (PointHilbert pointHilbert : pointsWithHilbertValues) {
            System.out.println("点: (" + pointHilbert.point[0] + ", " + pointHilbert.point[1] + ") -> Hilbert 值: " + pointHilbert.hilbertValue);
        }
    }
}
