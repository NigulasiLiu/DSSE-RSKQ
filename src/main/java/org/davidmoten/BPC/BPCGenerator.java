package org.davidmoten.BPC;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Stream;

public class BPCGenerator {

    /**
     * 获取BPC值的映射。
     *
     * @param R    输入的BigInteger数组
     * @param bits 最大位数
     * @return 包含每次迭代结果的Map，其中键是迭代次数，值是该轮保留的BigInteger列表
     */
    public static Map<Integer, List<BigInteger>> GetBPCValueMap(BigInteger[] R, int bits) {
        Map<Integer, List<BigInteger>> currentShiftCounts = new HashMap<>(); // 当前轮的右移次数
        // 当前轮的BigInteger集合
        Set<BigInteger> currentSet = new HashSet<>(Arrays.asList(R));
        int iteration = 0;
        // 终止条件：currentSet大小为1或达到bits次迭代
        while (currentSet.size() > 1 && iteration < bits) {
            Map<BigInteger, List<BigInteger>> map = new HashMap<>();
            // 构建父子节点映射关系
            for (BigInteger value : currentSet) {
                BigInteger parentValue = value.shiftRight(1); // 模拟父节点计算
                map.computeIfAbsent(parentValue, k -> new ArrayList<>()).add(value);
            }
            currentSet.clear(); // 清空currentSet，为下一轮迭代准备
            for (Map.Entry<BigInteger, List<BigInteger>> entry : map.entrySet()) {
                if (entry.getValue().size() > 1) {
                    // 如果当前父节点有多个子节点，则保留父节点
                    currentSet.add(entry.getKey());
                } else {
                    // 在iteration+1轮的迭代中，假如entry.getValue().size()=1，那么该元素是要被提取出来的
                    currentShiftCounts.computeIfAbsent(iteration, k -> new ArrayList<>()).addAll(entry.getValue());
                }
            }
            iteration++; // 增加迭代次数
        }
        // set中的最后一个元素
        currentShiftCounts.computeIfAbsent(iteration, k -> new ArrayList<>()).addAll(currentSet);
        return currentShiftCounts;
    }

    /**
     * 将BigInteger值转换为Order位的二进制字符串，并生成具有最佳前缀的字符串列表。
     *
     * @param resultMap 包含每次迭代结果的Map<Integer, List<BigInteger>>
     * @param bits      最大位数
     * @return 最佳前缀的二进制字符串列表
     */
    public static List<String> convertMapToPrefixString(Map<Integer, List<BigInteger>> resultMap, int bits) {
        List<String> binaryStrings = new ArrayList<>();

        // 遍历每次迭代的结果
        for (Map.Entry<Integer, List<BigInteger>> entry : resultMap.entrySet()) {
            int iteration = entry.getKey();
            List<BigInteger> values = entry.getValue();

            for (BigInteger value : values) {
                // 将 BigInteger 转换为二进制字符串
                String binaryString;
                if (!value.equals(BigInteger.ZERO)) {
                    binaryString = value.toString(2);
                } else {
                    binaryString = "";
                }

                // 计算有效长度和前缀位数
                int effectiveLength = bits - iteration;
                int leadingZeros = Math.max(effectiveLength - binaryString.length(), 0);

                // 构建完整的二进制字符串（前导0 + 二进制值 + '*')
                StringBuilder sb = new StringBuilder(bits);
                while (leadingZeros-- > 0) {
                    sb.append('0');
                }
                sb.append(binaryString);
                for (int i = 0; i < iteration; i++) {
                    sb.append('*');
                }

                // 添加结果到列表
                binaryStrings.add(sb.toString());
            }
        }

        return binaryStrings;
    }
    public static List<String> convertToOnlyPrefix(Map<Integer, List<BigInteger>> resultMap, int bits) {
        List<String> binaryStrings = new ArrayList<>();

        // 遍历每次迭代的结果
        for (Map.Entry<Integer, List<BigInteger>> entry : resultMap.entrySet()) {
            int iteration = entry.getKey();
            List<BigInteger> values = entry.getValue();

            for (BigInteger value : values) {
                // 将 BigInteger 转换为二进制字符串
                String binaryString;
                if (!value.equals(BigInteger.ZERO)) {
                    binaryString = value.toString(2);
                } else {
                    binaryString = "";
                }

                // 计算有效长度和前缀位数
                int effectiveLength = bits - iteration;
                int leadingZeros = Math.max(effectiveLength - binaryString.length(), 0);

                // 构建完整的二进制字符串（前导0 + 二进制值 + '*')
                StringBuilder sb = new StringBuilder(bits);
                while (leadingZeros-- > 0) {
                    sb.append('0');
                }
                sb.append(binaryString);

                // 添加结果到列表
                binaryStrings.add(sb.toString());
            }
        }

        return binaryStrings;
    }
    public static void main(String[] args) {
        int bits = 3;
        BigInteger[] R1 = {new BigInteger("28"), new BigInteger("35")};
        BigInteger[] R2 = {new BigInteger("56"), new BigInteger("59")};

        //生成min到max的所有Bigint
        BigInteger[] R = Stream.iterate(R1[0], n -> n.add(BigInteger.ONE))
                .limit(R1[1].subtract(R1[0]).add(BigInteger.ONE).intValueExact())
                .toArray(BigInteger[]::new);

        System.out.println("R:");
        for (BigInteger r : R) {
            System.out.print(r + " ");
        }
        Map<Integer, List<BigInteger>> results1 = BPCGenerator.GetBPCValueMap(R,bits);
        System.out.println("\nBPC2:" + BPCGenerator.convertMapToPrefixString(results1,bits));
        System.out.println("\nBPC3:" + BPCGenerator.convertToOnlyPrefix(results1,bits));
//        List<BigInteger> results = bpc.GetBPCValueList(R);
//        List<String> BinaryResults = new ArrayList<>();
////        System.out.println("BPC1: " + results);
//        for (BigInteger result : results) {
//            String bpc_string = bpc.toBinaryStringWithStars(result, bits, bpc.shiftCounts.get(result));
//            BinaryResults.add(bpc_string);
//        }
//        System.out.println("\nBPC1:" + BinaryResults);
        // 获取BPC结果（包括分组）
//        System.out.println("\nbpc_string:");
//        int groupIndex = 1; // 分组计数器

//        for (Map.Entry<Integer, List<BigInteger>> entry : results1.entrySet()) {
//            System.out.println("Group " + groupIndex + " (Iteration " + entry.getKey() + "):");
//            System.out.println(entry.getValue());
//            groupIndex++;
//        }

    }
}
