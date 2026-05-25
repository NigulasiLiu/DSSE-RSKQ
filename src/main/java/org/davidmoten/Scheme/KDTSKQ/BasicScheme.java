package org.davidmoten.Scheme.KDTSKQ;

import org.davidmoten.Hilbert.HilbertComponent.HilbertCurve;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

public class BasicScheme {
    private VHRSSE hilbertIndex;
    private VHRSSE timeIndex;
    private VHRSSE keywordIndex;
    private HilbertCurve hilbertCurve;
    private int L;

    public BasicScheme(int L, int hilbertBits) {
        this.L = L;
        this.hilbertIndex = VHRSSE.Setup(L);
        this.timeIndex = VHRSSE.Setup(L);
        this.keywordIndex = VHRSSE.Setup(L);
        this.hilbertCurve = HilbertCurve.bits(hilbertBits).dimensions(2);
    }

    public void build(List<Record> dataset) {
        Map<String, List<Integer>> hMap = new HashMap<>();
        Map<String, List<Integer>> tMap = new HashMap<>();
        Map<String, List<Integer>> kwMap = new HashMap<>();

        for (Record r : dataset) {
            String hKey = hilbertCurve.index(r.x, r.y).toString();
            hMap.computeIfAbsent(hKey, k -> new ArrayList<>()).add(r.id);

            String tKey = String.valueOf(r.timestamp);
            tMap.computeIfAbsent(tKey, k -> new ArrayList<>()).add(r.id);

            for (String kw : r.W) {
                kwMap.computeIfAbsent(kw, k -> new ArrayList<>()).add(r.id);
            }
        }

        build1DIndex(hilbertIndex, hMap);
        build1DIndex(timeIndex, tMap);
        build1DIndex(keywordIndex, kwMap);
    }

    private void build1DIndex(VHRSSE index, Map<String, List<Integer>> invertedMap) {
        List<String> sortedKeys = invertedMap.keySet().stream()
                .map(BigInteger::new)
                .sorted()
                .map(BigInteger::toString)
                .collect(Collectors.toList());
        index.buildIndex(invertedMap, sortedKeys);
    }

    public Set<Integer> search(int minX, int maxX, int minY, int maxY, int t1, int t2, List<String> W_Q) throws Exception {
        List<BigInteger[]> hilbertIntervals = extractHilbertIntervals(minX, maxX, minY, maxY);
        Set<Integer> spatialDocs = new HashSet<>();
        for (BigInteger[] interval : hilbertIntervals) {
            String[] range = new String[]{interval[0].toString(), interval[1].toString()};
            List<String> tokens = hilbertIndex.genToken(range);
            if (!tokens.isEmpty()) {
                List<byte[]> encRes = hilbertIndex.searchTokens(tokens);
                spatialDocs.addAll(hilbertIndex.localSearch(encRes, tokens));
            }
        }

        List<String> tTokens = timeIndex.genToken(new String[]{String.valueOf(t1), String.valueOf(t2)});
        List<byte[]> tRes = timeIndex.searchTokens(tTokens);
        Set<Integer> timeDocs = new HashSet<>(timeIndex.localSearch(tRes, tTokens));

        Set<Integer> wordDocs = null;
        for (String kw : W_Q) {
            String[] range = new String[]{kw, kw};
            List<String> tokens = keywordIndex.genToken(range);
            Set<Integer> kwDocs = new HashSet<>();
            if (!tokens.isEmpty()) {
                List<byte[]> encRes = keywordIndex.searchTokens(tokens);
                kwDocs.addAll(keywordIndex.localSearch(encRes, tokens));
            }
            if (wordDocs == null) wordDocs = kwDocs;
            else wordDocs.retainAll(kwDocs);
        }

        Set<Integer> finalResult = new HashSet<>(spatialDocs);
        finalResult.retainAll(timeDocs);
        if (wordDocs != null) finalResult.retainAll(wordDocs);

        return finalResult;
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
}
