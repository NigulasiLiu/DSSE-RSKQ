package org.davidmoten.Scheme.KDTSKQ;

import java.util.*;
import java.util.stream.Collectors;

class Record {
    int id;
    int x, y;
    int timestamp; // 0 to 86400
    List<String> W;
}

public class BasicScheme {
    private VHRSSE xIndex, yIndex, tIndex;
    private Map<String, List<Integer>> wIndex; // 关键字索引 (实际中也应加密)
    private int L;

    public BasicScheme(int L) {
        this.L = L;
        this.xIndex = VHRSSE.Setup(L);
        this.yIndex = VHRSSE.Setup(L);
        this.tIndex = VHRSSE.Setup(L);
        this.wIndex = new HashMap<>();
    }

    // 提取并构建 VHRSSE 所需的倒排表和有序关键字列表
    private void build1DIndex(VHRSSE index, Map<String, List<Integer>> invertedMap) {
        List<String> sortedKeys = invertedMap.keySet().stream()
                .map(Long::parseLong)
                .sorted()
                .map(String::valueOf)
                .collect(Collectors.toList());
        index.buildIndex(invertedMap, sortedKeys);
    }

    public void build(List<Record> dataset) {
        Map<String, List<Integer>> xMap = new HashMap<>();
        Map<String, List<Integer>> yMap = new HashMap<>();
        Map<String, List<Integer>> tMap = new HashMap<>();

        for (Record r : dataset) {
            xMap.computeIfAbsent(String.valueOf(r.x), k -> new ArrayList<>()).add(r.id);
            yMap.computeIfAbsent(String.valueOf(r.y), k -> new ArrayList<>()).add(r.id);
            tMap.computeIfAbsent(String.valueOf(r.timestamp), k -> new ArrayList<>()).add(r.id);

            for (String kw : r.W) {
                wIndex.computeIfAbsent(kw, k -> new ArrayList<>()).add(r.id);
            }
        }

        build1DIndex(xIndex, xMap);
        build1DIndex(yIndex, yMap);
        build1DIndex(tIndex, tMap);
    }

    public Set<Integer> search(int minX, int maxX, int minY, int maxY, int t1, int t2, List<String> W_Q) throws Exception {
        // 1. 坐标 X 查询
        List<String> xTokens = xIndex.genToken(new String[]{String.valueOf(minX), String.valueOf(maxX)});
        List<byte[]> xRes = xIndex.searchTokens(xTokens);
        Set<Integer> xDocs = new HashSet<>(xIndex.localSearch(xRes, xTokens));

        // 2. 坐标 Y 查询
        List<String> yTokens = yIndex.genToken(new String[]{String.valueOf(minY), String.valueOf(maxY)});
        List<byte[]> yRes = yIndex.searchTokens(yTokens);
        Set<Integer> yDocs = new HashSet<>(yIndex.localSearch(yRes, yTokens));

        // 3. Timestamp 查询
        List<String> tTokens = tIndex.genToken(new String[]{String.valueOf(t1), String.valueOf(t2)});
        List<byte[]> tRes = tIndex.searchTokens(tTokens);
        Set<Integer> tDocs = new HashSet<>(tIndex.localSearch(tRes, tTokens));

        // 4. 关键字 W 查询 (需全部满足 W_Q)
        Set<Integer> wDocs = null;
        for (String kw : W_Q) {
            Set<Integer> kwRes = new HashSet<>(wIndex.getOrDefault(kw, new ArrayList<>()));
            if (wDocs == null) wDocs = kwRes;
            else wDocs.retainAll(kwRes);
        }

        // 5. 客户端本地取交集
        Set<Integer> finalResult = new HashSet<>(xDocs);
        finalResult.retainAll(yDocs);
        finalResult.retainAll(tDocs);
        if (wDocs != null) finalResult.retainAll(wDocs);

        return finalResult;
    }
}
