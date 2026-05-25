package org.davidmoten.Scheme.KDTSKQ;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

class SpatialKDNode {
    int minX, maxX, minY, maxY;
    SpatialKDNode left, right;
    boolean isLeaf;
    VHRSSE hilbertIndex;
}

public class ExtraSchemeV2 {
    private int L;
    private HilbertCurve hilbertCurve;

    private SpatialKDNode spatialRoot;

    private VHRSSE globalTimeIndex;
    private VHRSSE keywordVHRSSE;
    private Map<String, List<Integer>> keywordInvertedIndex;

    public Map<String, byte[]> GLOBAL_SERVER_EDB;

    public ExtraSchemeV2(int L, int hilbertBits) {
        this.L = L;
        this.hilbertCurve = HilbertCurve.bits(hilbertBits).dimensions(2);
        this.keywordInvertedIndex = new HashMap<>();
        this.GLOBAL_SERVER_EDB = new HashMap<>();
    }

    public void build(List<Record> dataset) {
        int[] sBounds = getSpatialBounds(dataset);
        spatialRoot = buildSpatialKDTree(dataset, sBounds[0], sBounds[1], sBounds[2], sBounds[3], 0);

        Map<String, List<Integer>> tMap = new HashMap<>();
        for (Record r : dataset) {
            String tKey = "T_" + r.timestamp;
            tMap.computeIfAbsent(tKey, k -> new ArrayList<>()).add(r.id);
        }
        globalTimeIndex = VHRSSE.Setup(L);
        List<String> sortedTKeys = tMap.keySet().stream().sorted().collect(Collectors.toList());
        globalTimeIndex.buildIndex(tMap, sortedTKeys);
        GLOBAL_SERVER_EDB.putAll(globalTimeIndex.EDB);

        for (Record r : dataset) {
            for (String kw : r.W) {
                keywordInvertedIndex.computeIfAbsent("KW_" + kw, k -> new ArrayList<>()).add(r.id);
            }
        }
        keywordVHRSSE = VHRSSE.Setup(L);
        List<String> sortedKeywords = keywordInvertedIndex.keySet().stream().sorted().collect(Collectors.toList());
        keywordVHRSSE.buildIndex(keywordInvertedIndex, sortedKeywords);
        GLOBAL_SERVER_EDB.putAll(keywordVHRSSE.EDB);
    }

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
                    .map(s -> new BigInteger(s.substring(2)))
                    .sorted()
                    .map(b -> "H_" + b.toString())
                    .collect(Collectors.toList());

            node.hilbertIndex.buildIndex(hMap, sortedHKeys);
            GLOBAL_SERVER_EDB.putAll(node.hilbertIndex.EDB);
            return node;
        }

        node.isLeaf = false;
        List<Record> leftDocs = new ArrayList<>();
        List<Record> rightDocs = new ArrayList<>();

        if (depth % 2 == 0) {
            docs.sort(Comparator.comparingInt(r -> r.x));
            int midX = docs.get(docs.size() / 2).x;
            for (Record r : docs) {
                if (r.x <= midX) leftDocs.add(r); else rightDocs.add(r);
            }
            node.left = buildSpatialKDTree(leftDocs, minX, midX, minY, maxY, depth + 1);
            node.right = buildSpatialKDTree(rightDocs, midX + 1, maxX, minY, maxY, depth + 1);
        } else {
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

    public Set<Integer> search(int qMinX, int qMaxX, int qMinY, int qMaxY, int qMinT, int qMaxT, List<String> W_Q) throws Exception {
        Set<Integer> spatialDocs = new HashSet<>();
        searchSpatial(spatialRoot, qMinX, qMaxX, qMinY, qMaxY, spatialDocs);

        Set<Integer> timeDocs = searchTime(qMinT, qMaxT);

        Set<Integer> wordDocs = searchKeywords(W_Q);

        Set<Integer> finalResult = new HashSet<>(spatialDocs);
        finalResult.retainAll(timeDocs);
        if (wordDocs != null) {
            finalResult.retainAll(wordDocs);
        }

        return finalResult;
    }

    private Set<Integer> searchTime(int qMinT, int qMaxT) throws Exception {
        String[] range = new String[]{"T_" + qMinT, "T_" + qMaxT};
        List<String> tokens = globalTimeIndex.genToken(range);
        if (tokens.isEmpty()) return new HashSet<>();
        List<byte[]> encRes = fetchFromServer(tokens);
        return new HashSet<>(globalTimeIndex.localSearch(encRes, tokens));
    }

    private void searchSpatial(SpatialKDNode node, int qMinX, int qMaxX, int qMinY, int qMaxY, Set<Integer> result) throws Exception {
        if (qMaxX < node.minX || qMinX > node.maxX || qMaxY < node.minY || qMinY > node.maxY) return;

        if (node.isLeaf) {
            int ixMin = Math.max(qMinX, node.minX);
            int ixMax = Math.min(qMaxX, node.maxX);
            int iyMin = Math.max(qMinY, node.minY);
            int iyMax = Math.min(qMaxY, node.maxY);

            List<BigInteger[]> intervals = extractHilbertIntervals(ixMin, ixMax, iyMin, iyMax);

            for (BigInteger[] interval : intervals) {
                String[] range = new String[]{"H_" + interval[0].toString(), "H_" + interval[1].toString()};
                List<String> tokens = node.hilbertIndex.genToken(range);

                if (!tokens.isEmpty()) {
                    List<byte[]> encRes = fetchFromServer(tokens);
                    List<Integer> docs = node.hilbertIndex.localSearch(encRes, tokens);
                    result.addAll(docs);
                }
            }
        } else {
            searchSpatial(node.left, qMinX, qMaxX, qMinY, qMaxY, result);
            searchSpatial(node.right, qMinX, qMaxX, qMinY, qMaxY, result);
        }
    }

    private Set<Integer> searchKeywords(List<String> W_Q) throws Exception {
        if (W_Q == null || W_Q.isEmpty()) return null;

        Set<Integer> result = null;
        for (String kw : W_Q) {
            String[] range = new String[]{"KW_" + kw, "KW_" + kw};
            List<String> tokens = keywordVHRSSE.genToken(range);

            Set<Integer> kwDocs = new HashSet<>();
            if (!tokens.isEmpty()) {
                List<byte[]> encRes = fetchFromServer(tokens);
                kwDocs.addAll(keywordVHRSSE.localSearch(encRes, tokens));
            }

            if (result == null) result = kwDocs;
            else result.retainAll(kwDocs);
        }
        return result;
    }

    private List<byte[]> fetchFromServer(List<String> tokens) {
        List<byte[]> result = new ArrayList<>();
        for (String token : tokens) {
            if (GLOBAL_SERVER_EDB.containsKey(token)) {
                result.add(GLOBAL_SERVER_EDB.get(token));
            }
        }
        return result;
    }

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
            if (!curr.subtract(prev).equals(BigInteger.ONE)) {
                intervals.add(new BigInteger[]{start, prev});
                start = curr;
            }
            prev = curr;
        }
        intervals.add(new BigInteger[]{start, prev});
        return intervals;
    }

    public int[] getSpatialBounds(List<Record> dataset) {
        if (dataset == null || dataset.isEmpty()) return new int[]{0, 0, 0, 0};
        int minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;
        int minY = Integer.MAX_VALUE, maxY = Integer.MIN_VALUE;
        for (Record r : dataset) {
            if (r.x < minX) minX = r.x;
            if (r.x > maxX) maxX = r.x;
            if (r.y < minY) minY = r.y;
            if (r.y > maxY) maxY = r.y;
        }
        return new int[]{minX, maxX, minY, maxY};
    }
}
