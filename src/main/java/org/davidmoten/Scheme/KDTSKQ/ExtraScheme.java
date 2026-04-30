package org.davidmoten.Scheme.KDTSKQ;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

class KDNode {
    int minX, maxX, minY, maxY;
    KDNode left, right;
    boolean isLeaf;
    VHRSSE hilbertIndex; // 叶子节点独有的 Hilbert 1D-SSE
}

public class ExtraScheme {
    private int L;
    private HilbertCurve hilbertCurve;
    private KDNode root;
    private VHRSSE globalTIndex; // 全局时间戳索引
    private Map<String, List<Integer>> globalWIndex; // 全局关键字索引

    public ExtraScheme(int L, int hilbertBits) {
        this.L = L;
        this.hilbertCurve = HilbertCurve.bits(hilbertBits).dimensions(2);
        this.globalTIndex = VHRSSE.Setup(L); // 假设时间跨度不超 L 或适当调节
        this.globalWIndex = new HashMap<>();
    }

    // --- 构建阶段 ---
    public void build(List<Record> dataset, int minX, int maxX, int minY, int maxY) {
        // 构建全局 T 和 W 索引 (省略具体装载代码，同 Basic)
        // ...

        // 构建 K-D 树空间划分
        root = buildKDTree(dataset, minX, maxX, minY, maxY, 0);
    }

    private KDNode buildKDTree(List<Record> docs, int minX, int maxX, int minY, int maxY, int depth) {
        KDNode node = new KDNode();
        node.minX = minX; node.maxX = maxX;
        node.minY = minY; node.maxY = maxY;

        if (docs.size() <= L) {
            node.isLeaf = true;
            node.hilbertIndex = VHRSSE.Setup(L);

            // 构建分区内的 Hilbert 倒排索引
            Map<String, List<Integer>> hMap = new HashMap<>();
            for (Record r : docs) {
                String hIndex = hilbertCurve.index(r.x, r.y).toString();
                hMap.computeIfAbsent(hIndex, k -> new ArrayList<>()).add(r.id);
            }
            List<String> sortedHKeys = hMap.keySet().stream()
                    .map(BigInteger::new).sorted().map(BigInteger::toString)
                    .collect(Collectors.toList());

            node.hilbertIndex.buildIndex(hMap, sortedHKeys);
            return node;
        }

        node.isLeaf = false;
        // 简单二分逻辑，交替维度
        List<Record> leftDocs = new ArrayList<>();
        List<Record> rightDocs = new ArrayList<>();
        if (depth % 2 == 0) { // 按 X 划分
            int midX = minX + (maxX - minX) / 2;
            for (Record r : docs) {
                if (r.x <= midX) leftDocs.add(r);
                else rightDocs.add(r);
            }
            node.left = buildKDTree(leftDocs, minX, midX, minY, maxY, depth + 1);
            node.right = buildKDTree(rightDocs, midX + 1, maxX, minY, maxY, depth + 1);
        } else { // 按 Y 划分
            int midY = minY + (maxY - minY) / 2;
            for (Record r : docs) {
                if (r.y <= midY) leftDocs.add(r);
                else rightDocs.add(r);
            }
            node.left = buildKDTree(leftDocs, minX, maxX, minY, midY, depth + 1);
            node.right = buildKDTree(rightDocs, minX, maxX, midY + 1, maxY, depth + 1);
        }
        return node;
    }

    // --- 查询阶段 ---
    public Set<Integer> search(int qMinX, int qMaxX, int qMinY, int qMaxY, int t1, int t2, List<String> W_Q) throws Exception {
        Set<Integer> spatialDocs = new HashSet<>();
        searchSpatial(root, qMinX, qMaxX, qMinY, qMaxY, spatialDocs);

        // 获取时间与关键字集合 (与 Basic 类似调用，此处简写)
        // Set<Integer> timeDocs = queryGlobalTime(t1, t2);
        // Set<Integer> wordDocs = queryGlobalWords(W_Q);

        // 取交集
        // spatialDocs.retainAll(timeDocs);
        // spatialDocs.retainAll(wordDocs);
        return spatialDocs;
    }

    private void searchSpatial(KDNode node, int qMinX, int qMaxX, int qMinY, int qMaxY, Set<Integer> result) throws Exception {
        // 判断是否相交
        if (qMaxX < node.minX || qMinX > node.maxX || qMaxY < node.minY || qMinY > node.maxY) {
            return;
        }

        if (node.isLeaf) {
            // 计算查询框与该分区的交集矩形
            int ixMin = Math.max(qMinX, node.minX);
            int ixMax = Math.min(qMaxX, node.maxX);
            int iyMin = Math.max(qMinY, node.minY);
            int iyMax = Math.min(qMaxY, node.maxY);

            // 提取交集矩形内的 Hilbert 连续区间
            List<BigInteger[]> intervals = extractHilbertIntervals(ixMin, ixMax, iyMin, iyMax);

            // 对每个连续区间执行 VH-RSSE 查询
            for (BigInteger[] interval : intervals) {
                String[] range = new String[]{interval[0].toString(), interval[1].toString()};
                List<String> tokens = node.hilbertIndex.genToken(range);

                if (!tokens.isEmpty()) {
                    List<byte[]> encRes = node.hilbertIndex.searchTokens(tokens);
                    List<Integer> docs = node.hilbertIndex.localSearch(encRes, tokens);
                    result.addAll(docs);
                }
            }
        } else {
            searchSpatial(node.left, qMinX, qMaxX, qMinY, qMaxY, result);
            searchSpatial(node.right, qMinX, qMaxX, qMinY, qMaxY, result);
        }
    }

    // 工具函数：将 2D 矩形转化为一维 Hilbert 连续区间
    private List<BigInteger[]> extractHilbertIntervals(int minX, int maxX, int minY, int maxY) {
        List<BigInteger> hValues = new ArrayList<>();
        for (int x = minX; x <= maxX; x++) {
            for (int y = minY; y <= maxY; y++) {
                hValues.add(hilbertCurve.index(x, y));
            }
        }
        Collections.sort(hValues);

        List<BigInteger[]> intervals = new ArrayList<>();
        if (hValues.isEmpty()) return intervals;

        BigInteger start = hValues.get(0);
        BigInteger prev = start;

        for (int i = 1; i < hValues.size(); i++) {
            BigInteger curr = hValues.get(i);
            // 如果不连续，则截断形成一个区间
            if (!curr.subtract(prev).equals(BigInteger.ONE)) {
                intervals.add(new BigInteger[]{start, prev});
                start = curr;
            }
            prev = curr;
        }
        intervals.add(new BigInteger[]{start, prev});
        return intervals;
    }
}