package org.davidmoten.Scheme.KDTSKQ;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;

public class ThreeTokenScheme {

    private int L;
    private byte[] key;
    private Function<byte[], byte[]> H1;
    private Function<byte[], byte[]> H2;
    private Map<String, byte[]> EDB;

    private TokenKDNode spatialRoot;
    private VHRSSE globalTimeIndex;
    private VHRSSE keywordIndex;
    private Map<String, List<Integer>> keywordInvertedIndex;

    public ThreeTokenScheme(int L) {
        this.L = L;
        byte[] k = new byte[16];
        new SecureRandom().nextBytes(k);
        this.key = k;

        this.H1 = (var data) -> {
            try {
                return MessageDigest.getInstance("SHA-256").digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        };
        this.H2 = (var data) -> {
            try {
                return MessageDigest.getInstance("SHA-384").digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        };

        this.EDB = new HashMap<>();
        this.keywordInvertedIndex = new HashMap<>();
    }

    static class TokenKDNode {
        int minX, maxX, minY, maxY;
        TokenKDNode left, right;
        boolean isLeaf;

        List<Integer> localDocIds;
        List<Integer> sortedXCoords;
        List<Integer> sortedYCoords;
        Map<String, byte[]> partitionEDB;
        Map<Long, Integer> idToPosition;
        Map<Integer, Integer> positionToRealId;
    }

    public void build(List<Record> dataset) {
        int[] bounds = getSpatialBounds(dataset);
        spatialRoot = buildKDTree(dataset, bounds[0], bounds[1], bounds[2], bounds[3], 0);

        Map<String, List<Integer>> tMap = new HashMap<>();
        for (Record r : dataset) {
            tMap.computeIfAbsent("T_" + r.timestamp, k -> new ArrayList<>()).add(r.id);
        }
        globalTimeIndex = VHRSSE.Setup(L);
        List<String> sortedTKeys = new ArrayList<>(tMap.keySet());
        Collections.sort(sortedTKeys);
        globalTimeIndex.buildIndex(tMap, sortedTKeys);
        EDB.putAll(globalTimeIndex.EDB);

        for (Record r : dataset) {
            for (String kw : r.W) {
                keywordInvertedIndex.computeIfAbsent("KW_" + kw, k -> new ArrayList<>()).add(r.id);
            }
        }
        keywordIndex = VHRSSE.Setup(L);
        List<String> sortedKwKeys = new ArrayList<>(keywordInvertedIndex.keySet());
        Collections.sort(sortedKwKeys);
        keywordIndex.buildIndex(keywordInvertedIndex, sortedKwKeys);
        EDB.putAll(keywordIndex.EDB);
    }

    private TokenKDNode buildKDTree(List<Record> docs, int minX, int maxX, int minY, int maxY, int depth) {
        TokenKDNode node = new TokenKDNode();
        node.minX = minX;
        node.maxX = maxX;
        node.minY = minY;
        node.maxY = maxY;

        if (docs.size() <= L) {
            node.isLeaf = true;
            node.localDocIds = new ArrayList<>();
            node.idToPosition = new HashMap<>();
            node.positionToRealId = new HashMap<>();
            node.partitionEDB = new HashMap<>();

            for (int i = 0; i < docs.size(); i++) {
                Record r = docs.get(i);
                node.localDocIds.add(r.id);
                node.idToPosition.put((long) r.id, i);
                node.positionToRealId.put(i, r.id);
            }

            Set<Integer> xSet = new TreeSet<>();
            Set<Integer> ySet = new TreeSet<>();
            for (Record r : docs) {
                xSet.add(r.x);
                ySet.add(r.y);
            }
            node.sortedXCoords = new ArrayList<>(xSet);
            node.sortedYCoords = new ArrayList<>(ySet);

            for (int u : node.sortedXCoords) {
                for (int v : node.sortedYCoords) {
                    byte[] bitmap = build2DCumulativeBitmap(docs, u, v);

                    String indexKey = nodeHashCode(node) + "|" + u + "|" + v;
                    byte[] otpStream = H1.apply((indexKey).getBytes());
                    byte[] encBitmap = xorBytes(bitmap, otpStream, L);
                    String token = bytesToHex(H2.apply(indexKey.getBytes()));

                    node.partitionEDB.put(token, encBitmap);
                    EDB.put(token, encBitmap);
                }
            }

            String mapKey = "MAP_" + nodeHashCode(node);
            byte[] mapOtp = H1.apply(mapKey.getBytes());
            byte[] mapData = encodeIdMap(node.positionToRealId, docs.size());
            byte[] encMap = xorBytes(mapData, mapOtp, Math.max(mapData.length, mapOtp.length));
            String mapToken = bytesToHex(H2.apply(mapKey.getBytes()));
            node.partitionEDB.put(mapToken, encMap);
            EDB.put(mapToken, encMap);

            return node;
        }

        node.isLeaf = false;
        List<Record> leftDocs = new ArrayList<>();
        List<Record> rightDocs = new ArrayList<>();

        if (depth % 2 == 0) {
            docs.sort(Comparator.comparingInt(r -> r.x));
            int midX = docs.get(docs.size() / 2).x;
            for (Record r : docs) {
                if (r.x <= midX) leftDocs.add(r);
                else rightDocs.add(r);
            }
            node.left = buildKDTree(leftDocs, minX, midX, minY, maxY, depth + 1);
            node.right = buildKDTree(rightDocs, midX + 1, maxX, minY, maxY, depth + 1);
        } else {
            docs.sort(Comparator.comparingInt(r -> r.y));
            int midY = docs.get(docs.size() / 2).y;
            for (Record r : docs) {
                if (r.y <= midY) leftDocs.add(r);
                else rightDocs.add(r);
            }
            node.left = buildKDTree(leftDocs, minX, maxX, minY, midY, depth + 1);
            node.right = buildKDTree(rightDocs, minX, maxX, midY + 1, maxY, depth + 1);
        }
        return node;
    }

    private byte[] build2DCumulativeBitmap(List<Record> docs, int u, int v) {
        StringBuilder sb = new StringBuilder();
        for (Record r : docs) {
            sb.append((r.x <= u && r.y <= v) ? '1' : '0');
        }
        int remaining = L - docs.size();
        for (int i = 0; i < remaining; i++) {
            sb.append('0');
        }
        return sb.toString().getBytes();
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

    private void searchSpatial(TokenKDNode node, int qMinX, int qMaxX, int qMinY, int qMaxY, Set<Integer> result) throws Exception {
        if (qMaxX < node.minX || qMinX > node.maxX || qMaxY < node.minY || qMinY > node.maxY) return;

        if (node.isLeaf) {
            int ixMin = Math.max(qMinX, node.minX);
            int ixMax = Math.min(qMaxX, node.maxX);
            int iyMin = Math.max(qMinY, node.minY);
            int iyMax = Math.min(qMaxY, node.maxY);

            int uTR = findLargestLe(node.sortedXCoords, ixMax);
            int vTR = findLargestLe(node.sortedYCoords, iyMax);
            Integer uTL = findLargestLt(node.sortedXCoords, ixMin);
            Integer vBR = findLargestLt(node.sortedYCoords, iyMin);

            String nodeHash = String.valueOf(nodeHashCode(node));

            String trKey = nodeHash + "|" + uTR + "|" + vTR;
            String trToken = bytesToHex(H2.apply(trKey.getBytes()));
            byte[] eTR = EDB.get(trToken);
            if (eTR == null) return;
            byte[] bTR = xorBytes(eTR, H1.apply(trKey.getBytes()), L);

            byte[] bTL = new byte[L];
            if (uTL != null) {
                String tlKey = nodeHash + "|" + uTL + "|" + vTR;
                String tlToken = bytesToHex(H2.apply(tlKey.getBytes()));
                byte[] eTL = EDB.get(tlToken);
                if (eTL != null) {
                    bTL = xorBytes(eTL, H1.apply(tlKey.getBytes()), L);
                }
            }

            byte[] bBR = new byte[L];
            if (vBR != null) {
                String brKey = nodeHash + "|" + uTR + "|" + vBR;
                String brToken = bytesToHex(H2.apply(brKey.getBytes()));
                byte[] eBR = EDB.get(brToken);
                if (eBR != null) {
                    bBR = xorBytes(eBR, H1.apply(brKey.getBytes()), L);
                }
            }

            byte[] bRes = inclusionExclusion(bTR, bTL, bBR);

            String mapKey = "MAP_" + nodeHash;
            String mapToken = bytesToHex(H2.apply(mapKey.getBytes()));
            byte[] encMap = EDB.get(mapToken);
            if (encMap != null) {
                byte[] mapOtp = H1.apply(mapKey.getBytes());
                byte[] mapData = xorBytes(encMap, mapOtp, Math.max(encMap.length, mapOtp.length));
                Map<Integer, Integer> posToId = decodeIdMap(mapData, node.localDocIds.size());

                for (int i = 0; i < Math.min(bRes.length, node.localDocIds.size()); i++) {
                    if (bRes[i] == 1) {
                        Integer realId = posToId.get(i);
                        if (realId != null) result.add(realId);
                    }
                }
            }
        } else {
            searchSpatial(node.left, qMinX, qMaxX, qMinY, qMaxY, result);
            searchSpatial(node.right, qMinX, qMaxX, qMinY, qMaxY, result);
        }
    }

    private byte[] inclusionExclusion(byte[] bTR, byte[] bTL, byte[] bBR) {
        int len = Math.min(Math.min(bTR.length, bTL.length), bBR.length);
        byte[] result = new byte[len];
        for (int i = 0; i < len; i++) {
            int tr = (bTR[i] == '1' || bTR[i] == 1) ? 1 : 0;
            int tl = (bTL[i] == '1' || bTL[i] == 1) ? 1 : 0;
            int br = (bBR[i] == '1' || bBR[i] == 1) ? 1 : 0;
            result[i] = (byte) (tr & (~(tl | br) & 1));
        }
        return result;
    }

    private Set<Integer> searchTime(int qMinT, int qMaxT) throws Exception {
        String[] range = new String[]{"T_" + qMinT, "T_" + qMaxT};
        List<String> tokens = globalTimeIndex.genToken(range);
        if (tokens.isEmpty()) return new HashSet<>();
        List<byte[]> encRes = fetchFromServer(tokens);
        return new HashSet<>(globalTimeIndex.localSearch(encRes, tokens));
    }

    private Set<Integer> searchKeywords(List<String> W_Q) throws Exception {
        if (W_Q == null || W_Q.isEmpty()) return null;
        Set<Integer> result = null;
        for (String kw : W_Q) {
            String[] range = new String[]{"KW_" + kw, "KW_" + kw};
            List<String> tokens = keywordIndex.genToken(range);
            Set<Integer> kwDocs = new HashSet<>();
            if (!tokens.isEmpty()) {
                List<byte[]> encRes = fetchFromServer(tokens);
                kwDocs.addAll(keywordIndex.localSearch(encRes, tokens));
            }
            if (result == null) result = kwDocs;
            else result.retainAll(kwDocs);
        }
        return result;
    }

    private List<byte[]> fetchFromServer(List<String> tokens) {
        List<byte[]> result = new ArrayList<>();
        for (String token : tokens) {
            if (EDB.containsKey(token)) {
                result.add(EDB.get(token));
            }
        }
        return result;
    }

    private int findLargestLe(List<Integer> sorted, int value) {
        int idx = Collections.binarySearch(sorted, value);
        if (idx >= 0) return sorted.get(idx);
        int insertPos = -idx - 1;
        if (insertPos == 0) return sorted.get(0);
        return sorted.get(insertPos - 1);
    }

    private Integer findLargestLt(List<Integer> sorted, int value) {
        int idx = Collections.binarySearch(sorted, value);
        if (idx > 0) return sorted.get(idx - 1);
        int insertPos = (idx < 0) ? -idx - 1 : idx;
        if (insertPos == 0) return null;
        return sorted.get(insertPos - 1);
    }

    private byte[] encodeIdMap(Map<Integer, Integer> posToId, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            Integer id = posToId.get(i);
            sb.append(id != null ? id : 0);
            if (i < count - 1) sb.append(",");
        }
        return sb.toString().getBytes();
    }

    private Map<Integer, Integer> decodeIdMap(byte[] data, int count) {
        Map<Integer, Integer> map = new HashMap<>();
        String s = new String(data).trim();
        if (s.isEmpty()) return map;
        String[] parts = s.split(",");
        for (int i = 0; i < Math.min(parts.length, count); i++) {
            try {
                map.put(i, Integer.parseInt(parts[i].trim()));
            } catch (NumberFormatException e) {
                break;
            }
        }
        return map;
    }

    private byte[] xorBytes(byte[] a, byte[] b, int len) {
        if (len <= 0) return new byte[0];
        int maxLen = Math.max(a.length, b.length);
        len = Math.min(len, maxLen);
        byte[] ra = new byte[len];
        byte[] rb = new byte[len];
        System.arraycopy(a, Math.max(0, a.length - len), ra, Math.max(0, len - a.length), Math.min(a.length, len));
        System.arraycopy(b, Math.max(0, b.length - len), rb, Math.max(0, len - b.length), Math.min(b.length, len));
        byte[] result = new byte[len];
        for (int i = 0; i < len; i++) {
            result[i] = (byte) (ra[i] ^ rb[i]);
        }
        return result;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private int nodeHashCode(TokenKDNode node) {
        return System.identityHashCode(node);
    }

    private int[] getSpatialBounds(List<Record> dataset) {
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
