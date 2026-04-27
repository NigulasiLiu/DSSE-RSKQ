package org.davidmoten.Hilbert.HilbertApp;

public class HilbertCurveType {

    // 定义分形的种类
    enum HilbertType {
        STANDARD,        // 标准顺序
        ROTATED,         // 轴旋转
        FLIPPED_VERTICAL,  // 上下倒置
        ROTATED_FLIPPED   // 轴旋转左右倒置
    }

    // 查找阶数为order的Hilbert曲线的点n所在的分形种类
    public static HilbertType findHilbertType(int order, int n, boolean isStandardStart) {
        int size = 1 << order;  // 2^order，表示矩阵的大小
        HilbertType type = isStandardStart ? HilbertType.STANDARD : HilbertType.ROTATED;  // 根据参数确定初始分形类型

        // 遍历每一个阶层
        for (int s = size / 2; s > 0; s /= 2) {
            int rx = (n / 2) % 2;  // 判断横坐标翻转与否
            int ry = n % 2;        // 判断纵坐标翻转与否

            // 根据当前的类型和rx, ry的值来更新类型
            switch (type) {
                case STANDARD:
                    if (rx == 0 && ry == 1) type = HilbertType.FLIPPED_VERTICAL;
                    else if (rx == 1 && ry == 0) type = HilbertType.ROTATED_FLIPPED;
                    else if (rx == 1 && ry == 1) type = HilbertType.ROTATED;
                    break;
                case ROTATED:
                    if (rx == 0 && ry == 0) type = HilbertType.STANDARD;
                    else if (rx == 0 && ry == 1) type = HilbertType.FLIPPED_VERTICAL;
                    break;
                case FLIPPED_VERTICAL:
                    if (rx == 0 && ry == 0) type = HilbertType.STANDARD;
                    else if (rx == 1 && ry == 0) type = HilbertType.ROTATED_FLIPPED;
                    break;
                case ROTATED_FLIPPED:
                    if (rx == 1 && ry == 1) type = HilbertType.ROTATED;
                    break;
            }
            n /= 4;
        }

        return type;
    }

    public static void main(String[] args) {
        int order = 4;  // 阶数
        int n = 2;      // 希尔伯特曲线中的点值
        boolean isStandardStart = true;  // 标记是否从标准顺序开始

        HilbertType type = findHilbertType(order, n, isStandardStart);
        System.out.println("点 " + n + " 在希尔伯特曲线中的分形种类为: " + type);
    }
}
