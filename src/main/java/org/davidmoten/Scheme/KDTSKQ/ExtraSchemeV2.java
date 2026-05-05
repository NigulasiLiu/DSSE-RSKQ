package org.davidmoten.Scheme.KDTSKQ;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

// --- 基础数据结构 ---

// 空间 K-D 树节点 (2D)
class SpatialKDNode {
    int minX, maxX, minY, maxY;
    SpatialKDNode left, right;
    boolean isLeaf;
    VHRSSE hilbertIndex;
}

// 时间 K-D 树节点 (1D)
class TimeKDNode {
    int minT, maxT;
    TimeKDNode left, right;
    boolean isLeaf;
    VHRSSE timeIndex;
}

public class ExtraSchemeV2 {
    private int L;
    private HilbertCurve hilbertCurve;

    // 客户端维护的本地树结构
    private SpatialKDNode spatialRoot;
    private TimeKDNode timeRoot;

    // 关键字精确匹配索引
    private Map<String, List<Integer>> keywordInvertedIndex;
    private VHRSSE keywordVHRSSE;

    // --- 核心优化：共享的全局云端状态 ---
    // 所有的加密位图最终合并到这里，交由 Server 托管。Server 完全不知道数据的维度含义。
    public Map<String, byte[]> GLOBAL_SERVER_EDB;

    public ExtraSchemeV2(int L, int hilbertBits) {
        this.L = L;
        this.hilbertCurve = HilbertCurve.bits(hilbertBits).dimensions(2);
        this.keywordInvertedIndex = new HashMap<>();
        this.GLOBAL_SERVER_EDB = new HashMap<>();
    }

    // ========================================================
    // 1. 构建阶段 (Build Phase)
    // ========================================================
    public void build(List<Record> dataset) {
        // 1.1 构建 2D 空间 KD 树
        int[] sBounds = getSpatialBounds(dataset);
        spatialRoot = buildSpatialKDTree(dataset, sBounds[0], sBounds[1], sBounds[2], sBounds[3], 0);

        // 1.2 构建 1D 时间 KD 树
        int[] tBounds = getTimeBounds(dataset);
        timeRoot = buildTimeKDTree(dataset, tBounds[0], tBounds[1]);

        // 1.3 构建 离散关键字 倒排索引
        for (Record r : dataset) {
            for (String kw : r.W) {
                // 加上前缀防止与其他维度的哈希碰撞
                keywordInvertedIndex.computeIfAbsent("KW_" + kw, k -> new ArrayList<>()).add(r.id);
            }
        }
        keywordVHRSSE = VHRSSE.Setup(L);
        List<String> sortedKeywords = keywordInvertedIndex.keySet().stream().sorted().collect(Collectors.toList());
        keywordVHRSSE.buildIndex(keywordInvertedIndex, sortedKeywords);

        // 1.4 【合并 EDB】(模拟上传至服务器)
        GLOBAL_SERVER_EDB.putAll(keywordVHRSSE.EDB);
    }

    // --- 1D 时间 K-D 树构建 ---
    private TimeKDNode buildTimeKDTree(List<Record> docs, int minT, int maxT) {
        TimeKDNode node = new TimeKDNode();
        node.minT = minT;
        node.maxT = maxT;

        if (docs.size() <= L) {
            node.isLeaf = true;
            node.timeIndex = VHRSSE.Setup(L);

            // 构建该分区内的时间索引
            Map<String, List<Integer>> tMap = new HashMap<>();
            for (Record r : docs) {
                // 加前缀隔离域
                String tKey = "T_" + r.timestamp;
                tMap.computeIfAbsent(tKey, k -> new ArrayList<>()).add(r.id);
            }
            List<String> sortedTKeys = tMap.keySet().stream().sorted().collect(Collectors.toList());
            node.timeIndex.buildIndex(tMap, sortedTKeys);

            // 将局部 EDB 并入全局 EDB
            GLOBAL_SERVER_EDB.putAll(node.timeIndex.EDB);
            return node;
        }

        node.isLeaf = false;
        // 对时间戳排序取中位数切分，保证绝对平衡
        docs.sort(Comparator.comparingInt(r -> r.timestamp));
        int midIndex = docs.size() / 2;
        int midT = docs.get(midIndex).timestamp;

        List<Record> leftDocs = new ArrayList<>(docs.subList(0, midIndex));
        List<Record> rightDocs = new ArrayList<>(docs.subList(midIndex, docs.size()));

        node.left = buildTimeKDTree(leftDocs, minT, midT);
        node.right = buildTimeKDTree(rightDocs, midT + 1, maxT);
        return node;
    }

    // --- 2D 空间 K-D 树构建 (带 Median 切分优化) ---
    private SpatialKDNode buildSpatialKDTree(List<Record> docs, int minX, int maxX, int minY, int maxY, int depth) {
        SpatialKDNode node = new SpatialKDNode();
        node.minX = minX; node.maxX = maxX;
        node.minY = minY; node.maxY = maxY;

        if (docs.size() <= L) {
            node.isLeaf = true;
            node.hilbertIndex = VHRSSE.Setup(L);

            Map<String, List<Integer>> hMap = new HashMap<>();
            for (Record r : docs) {
                String hKey = "H_" + hilbertCurve.index(r.x, r.y).toString();
                hMap.computeIfAbsent(hKey, k -> new ArrayList<>()).add(r.id);
            }
            List<String> sortedHKeys = hMap.keySet().stream()
                    // 必须先剥离前缀转 BigInteger 排序，再拼回前缀
                    .map(s -> new BigInteger(s.substring(2)))
                    .sorted()
                    .map(b -> "H_" + b.toString())
                    .collect(Collectors.toList());

            node.hilbertIndex.buildIndex(hMap, sortedHKeys);
            GLOBAL_SERVER_EDB.putAll(node.hilbertIndex.EDB); // 合并至全局
            return node;
        }

        node.isLeaf = false;
        List<Record> leftDocs = new ArrayList<>();
        List<Record> rightDocs = new ArrayList<>();

        if (depth % 2 == 0) { // 按 X 轴中位数切分
            docs.sort(Comparator.comparingInt(r -> r.x));
            int midX = docs.get(docs.size() / 2).x;
            for (Record r : docs) {
                if (r.x <= midX) leftDocs.add(r); else rightDocs.add(r);
            }
            node.left = buildSpatialKDTree(leftDocs, minX, midX, minY, maxY, depth + 1);
            node.right = buildSpatialKDTree(rightDocs, midX + 1, maxX, minY, maxY, depth + 1);
        } else { // 按 Y 轴中位数切分
            docs.sort(Comparator.comparingInt(r -> r.y));
            int midY = docs.get(docs.size() / 2).y;
            for (Record r : docs) {
                if (r.y <= midY) leftDocs.add(r); else rightDocs.add(r);
            }
            node.left = buildSpatialKDTree(leftDocs, minX, maxX, minY, midY, depth + 1);
            node.right = buildSpatialKDTree(rightDocs, minX, maxX, midY + 1, maxY, depth + 1);
        }
        return node;
    }

    // ========================================================
    // 2. 查询与交集阶段 (Search Phase)
    // ========================================================
    public Set<Integer> search(int qMinX, int qMaxX, int qMinY, int qMaxY, int qMinT, int qMaxT, List<String> W_Q) throws Exception {

        // 1. 独立搜索空间 K-D 树
        Set<Integer> spatialDocs = new HashSet<>();
        searchSpatial(spatialRoot, qMinX, qMaxX, qMinY, qMaxY, spatialDocs);

        // 2. 独立搜索时间 K-D 树
        Set<Integer> timeDocs = new HashSet<>();
        searchTime(timeRoot, qMinT, qMaxT, timeDocs);

        // 3. 独立搜索离散关键字
        Set<Integer> wordDocs = searchKeywords(W_Q);

        // 4. 客户端本地取交集 (Intersection)
        Set<Integer> finalResult = new HashSet<>(spatialDocs);
        finalResult.retainAll(timeDocs);
        if (wordDocs != null) {
            finalResult.retainAll(wordDocs);
        }

        return finalResult;
    }

    // --- 搜寻时间树 ---
    private void searchTime(TimeKDNode node, int qMinT, int qMaxT, Set<Integer> result) throws Exception {
        if (qMaxT < node.minT || qMinT > node.maxT) return; // 剪枝

        if (node.isLeaf) {
            // 裁剪有效时间查询域
            int tStart = Math.max(qMinT, node.minT);
            int tEnd = Math.min(qMaxT, node.maxT);

            String[] range = new String[]{"T_" + tStart, "T_" + tEnd};
            List<String> tokens = node.timeIndex.genToken(range);

            if (!tokens.isEmpty()) {
                // 注意：这里去模拟查询 GLOBAL_SERVER_EDB
                List<byte[]> encRes = fetchFromServer(tokens);
                List<Integer> docs = node.timeIndex.localSearch(encRes, tokens);
                result.addAll(docs);
            }
        } else {
            searchTime(node.left, qMinT, qMaxT, result);
            searchTime(node.right, qMinT, qMaxT, result);
        }
    }

    // --- 搜寻空间树 ---
    private void searchSpatial(SpatialKDNode node, int qMinX, int qMaxX, int qMinY, int qMaxY, Set<Integer> result) throws Exception {
        if (qMaxX < node.minX || qMinX > node.maxX || qMaxY < node.minY || qMinY > node.maxY) return; // 剪枝

        if (node.isLeaf) {
            int ixMin = Math.max(qMinX, node.minX);
            int ixMax = Math.min(qMaxX, node.maxX);
            int iyMin = Math.max(qMinY, node.minY);
            int iyMax = Math.min(qMaxY, node.maxY);

            // 此处省略 Hilbert 区间提取的逻辑实现细节
            List<BigInteger[]> intervals = extractHilbertIntervals(ixMin, ixMax, iyMin, iyMax);

            for (BigInteger[] interval : intervals) {
                String[] range = new String[]{"H_" + interval[0].toString(), "H_" + interval[1].toString()};
                List<String> tokens = node.hilbertIndex.genToken(range);

                if (!tokens.isEmpty()) {
                    List<byte[]> encRes = fetchFromServer(tokens); // 查询全局字典
                    List<Integer> docs = node.hilbertIndex.localSearch(encRes, tokens);
                    result.addAll(docs);
                }
            }
        } else {
            searchSpatial(node.left, qMinX, qMaxX, qMinY, qMaxY, result);
            searchSpatial(node.right, qMinX, qMaxX, qMinY, qMaxY, result);
        }
    }

    // --- 搜寻关键字 ---
    private Set<Integer> searchKeywords(List<String> W_Q) throws Exception {
        if (W_Q == null || W_Q.isEmpty()) return null;

        Set<Integer> result = null;
        for (String kw : W_Q) {
            String[] range = new String[]{"KW_" + kw, "KW_" + kw}; // 精确匹配转化
            List<String> tokens = keywordVHRSSE.genToken(range);

            Set<Integer> kwDocs = new HashSet<>();
            if (!tokens.isEmpty()) {
                List<byte[]> encRes = fetchFromServer(tokens);
                kwDocs.addAll(keywordVHRSSE.localSearch(encRes, tokens));
            }

            if (result == null) result = kwDocs;
            else result.retainAll(kwDocs); // AND 语义
        }
        return result;
    }

    // 模拟服务端响应：从全局盲化字典中提取密文
    private List<byte[]> fetchFromServer(List<String> tokens) {
        List<byte[]> result = new ArrayList<>();
        for (String token : tokens) {
            if (GLOBAL_SERVER_EDB.containsKey(token)) {
                result.add(GLOBAL_SERVER_EDB.get(token));
            }
        }
        return result;
    }

    // 省略获取边界的辅助函数 getSpatialBounds / getTimeBounds / extractHilbertIntervals
    public int[] getSpatialBounds(List<Record> dataset){
        //TODO
        return new int[0];
    }
}
