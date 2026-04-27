package org.davidmoten.Hilbert.HilbertApp;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;
import org.davidmoten.Hilbert.HilbertComponent.HilbertCurveRenderer;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.math.BigInteger;

public class HilbertCurveRenderApp {

    public static void main(String[] args) {
        // 这里可以直接指定维度和阶数
        int dimension = 2; // 2D空间
        int order = 2;     // 希尔伯特曲线的阶数

        // 测试点集
        long[][] points = {
                {0, 0},
                {1, 3},
                {10, 15},
                {31, 31}
        };

        // 调用渲染函数生成希尔伯特曲线并绘制点集
        generateHilbertCurve(dimension, order, points);

        // 计算并输出点的Hilbert编码值及颜色差异
        computeAndDisplayHilbertInfo(order, points);
    }

    private static void generateHilbertCurve(int dimension, int order, long[][] points) {
        if (dimension != 2) {
            System.err.println("This application currently supports only 2D Hilbert curves.");
            return;
        }

        try {
            // 生成希尔伯特曲线图像
            String filename = "hilbert_curve_d" + dimension + "_k" + order + ".png";
            int width = 800; // 设置图像宽度
            BufferedImage image = HilbertCurveRenderer.render(order, width, HilbertCurveRenderer.Option.COLORIZE, HilbertCurveRenderer.Option.LABEL);

            // 在图像上绘制点集
            drawPoints(image, order, points);

            // 保存图像
            ImageIO.write(image, "PNG", new File(filename));
            System.out.println("Hilbert curve with points generated and saved as: " + filename);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void drawPoints(BufferedImage image, int order, long[][] points) {
        Graphics2D g = image.createGraphics();
        g.setColor(Color.RED); // 使用红色绘制点
        int width = image.getWidth();
        int margin = 10;
        int n = 1 << order; // 计算2^order
        int cellSize = (width - 2 * margin) / n;

        for (long[] point : points) {
            int x = (int) Math.round((double) point[0] / (n - 1) * (width - 2 * margin - cellSize) + margin)
                    + cellSize / 2;
            int y = (int) Math.round((double) point[1] / (n - 1) * (width - 2 * margin - cellSize) + margin)
                    + cellSize / 2;
            g.fillOval(x - 5, y - 5, 10, 10); // 绘制一个半径为5的圆表示点
        }

        g.dispose(); // 释放Graphics2D对象
    }

    private static void computeAndDisplayHilbertInfo(int order, long[][] points) {
        HilbertCurve hilbertCurve = HilbertCurve.bits(order).dimensions(2);
        int n = 1 << order; // 计算2^order

        for (long[] point : points) {
            BigInteger index = hilbertCurve.index(point);
            Color color = calculateColor(index.longValue(), n);
            String hexColor = toHex(color);

            System.out.println("Point " + arrayToString(point) + " -> Hilbert Index: " + index + ", Color: #" + hexColor);
        }

        if (points.length >= 2) {
            BigInteger index1 = hilbertCurve.index(points[0]);
            BigInteger index2 = hilbertCurve.index(points[1]);
            Color color1 = calculateColor(index1.longValue(), n);
            Color color2 = calculateColor(index2.longValue(), n);
            int colorDifference = colorDifference(color1, color2);

            System.out.println("\nHex color difference between first two points: " + Integer.toHexString(colorDifference));
            analyzeColorDifference(colorDifference);
        }
    }

    private static Color calculateColor(long index, int n) {
        return Color.getHSBColor(((float) index) / (n * n), 0.5f, 1.0f);
    }

    private static String toHex(Color color) {
        return String.format("%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }

    private static int colorDifference(Color color1, Color color2) {
        int rDiff = Math.abs(color1.getRed() - color2.getRed());
        int gDiff = Math.abs(color1.getGreen() - color2.getGreen());
        int bDiff = Math.abs(color1.getBlue() - color2.getBlue());
        return (rDiff << 16) | (gDiff << 8) | bDiff;
    }

    private static void analyzeColorDifference(int colorDifference) {
        System.out.println("\nAnalysis of Color Difference:");

        if (colorDifference == 0) {
            System.out.println("The points are very close or identical on the Hilbert curve, indicating very similar or the same spatial locality.");
        } else if (colorDifference < 0x333333) {
            System.out.println("The points are relatively close on the Hilbert curve, suggesting they are in the same or adjacent spatial regions.");
        } else if (colorDifference < 0x666666) {
            System.out.println("The points have a moderate difference on the Hilbert curve, indicating they are somewhat separated in space.");
        } else {
            System.out.println("The points are far apart on the Hilbert curve, implying they represent significantly different spatial regions.");
        }

        System.out.println("\nImpact on Search Process:");
        if (colorDifference == 0) {
            System.out.println("If the points are in the same range of Hilbert indices, searching for them would likely involve similar or identical ranges.");
        } else {
            System.out.println("A larger color difference typically means that the points are farther apart in the spatial domain. In a search process, this could result in the need to search across multiple disjoint ranges on the Hilbert curve.");
            System.out.println("As the difference increases, the search process may require more computational effort to traverse these disjoint regions, especially if the underlying data structure (like a BitMp-tree) needs to perform multiple range lookups.");
        }
    }

    private static String arrayToString(long[] array) {
        return "[" + array[0] + ", " + array[1] + "]";
    }
}
