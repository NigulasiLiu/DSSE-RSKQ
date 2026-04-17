package org.davidmoten.Scheme.EPSRQ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * EPSRQ baseline (static) adapted to this workspace's dynamic benchmark loops.
 *
 * Mapping rules:
 * - h (Hilbert order) -> T (Gray code order)
 * - l (max files/capacity) -> gamma (padding threshold)
 *
 * Core matching rule:
 * - KeywordInnerProduct != 0 && LocationInnerProduct == 0
 */
public class EPSRQScheme {

    public static final class EncEntry {
        final int fileId; // -1 for phantom
        final EASPEUtils.CipherVector encLoc;
        EncEntry(int fileId, EASPEUtils.CipherVector encLoc) {
            this.fileId = fileId;
            this.encLoc = encLoc;
        }
    }

    private final int t;          // Gray code order
    private final int gamma;      // padding block size
    private final int maxFiles;   // used only for sanity mod
    private final Random rnd;

    private final Map<String, Integer> dict; // keyword -> index
    private int dictSize;

    private EASPEUtils.SecretKey skKeyword;
    private EASPEUtils.SecretKey skLocation;

    private static final class EncBlock {
        final List<EncEntry> entries = new ArrayList<>();
        int phantomCount = 0;
    }

    // per keyword: blocks; each block is padded to gamma via phantomCount (implicit)
    private final Map<String, List<EncBlock>> index;

    // cached encrypted keyword vector per keyword (EPKI list corresponds to that w)
    private final Map<String, EASPEUtils.CipherVector> encKeywordBasis;

    // time tracking (consistent shape with existing code)
    public final List<Double> totalUpdateTimes = new ArrayList<>();
    public final List<Double> clientSearchTimes = new ArrayList<>();
    public final List<Double> serverSearchTimes = new ArrayList<>();

    private static final double ZERO_EPS = 1e-6;

    public EPSRQScheme(int maxFiles, int h, int gamma) {
        this.t = h;
        this.gamma = Math.max(1, gamma);
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(2026L);
        this.dict = new HashMap<>();
        this.dictSize = 0;
        this.index = new HashMap<>();
        this.encKeywordBasis = new HashMap<>();
    }

    /**
     * Build static index from rows. This is the "from scratch" baseline setup.
     */
    public void buildIndex(List<Row> rows) {
        // build dictionary
        for (Row r : rows) {
            for (String w : r.keywords) {
                if (w == null) continue;
                dict.computeIfAbsent(w, k -> dictSize++);
            }
        }
        int dimKw = Math.max(1, dictSize);
        int dimLoc = 4 * t + 1;

        this.skKeyword = EASPEUtils.keyGen(dimKw, 1L);
        this.skLocation = EASPEUtils.keyGen(dimLoc, 2L);

        // build per keyword list
        for (Row r : rows) {
            updateInternal(r.x, r.y, r.keywords, "add", new int[]{r.fileId}, false);
        }

        // finalize padding for all keywords
        for (String w : new ArrayList<>(index.keySet())) ensurePadding(w);
    }

    /**
     * Dynamic update compromise:
     * plaintext encode -> EASPE encrypt -> append to in-memory list -> phantom padding check.
     */
    public double update(long[] pSet, String[] keywords, String op, int[] files) {
        long start = System.nanoTime();
        int x = (int) pSet[0];
        int y = (int) pSet[1];
        updateInternal(x, y, keywords, op, files, true);
        double ms = (System.nanoTime() - start) / 1e6;
        totalUpdateTimes.add(ms);
        return ms;
    }

    /**
     * Search using rectangle parameters already present in existing experiment loops.
     */
    public List<Integer> searchRect(int xStart, int yStart, int rangeLen, String[] queryKeywords) {
        long clientStart = System.nanoTime();
        // keyword query vector
        double[] qKw = keywordQueryVector(queryKeywords);
        EASPEUtils.CipherVector tdKw = EASPEUtils.trapdoor(skKeyword, qKw, rnd);

        // range decomposition (naive SGR: 1x1 per cell)
        List<EASPEUtils.CipherVector> tdRanges = new ArrayList<>();
        int max = 1 << t;
        int xEnd = Math.min(max - 1, xStart + Math.max(0, rangeLen));
        int yEnd = Math.min(max - 1, yStart + Math.max(0, rangeLen));
        for (int x = Math.max(0, xStart); x <= xEnd; x++) {
            for (int y = Math.max(0, yStart); y <= yEnd; y++) {
                double[] qLoc = GrayCodeEncoder.encodeCellRangeVector(x, y, t);
                tdRanges.add(EASPEUtils.trapdoor(skLocation, qLoc, rnd));
            }
        }
        long clientEnd = System.nanoTime();

        long serverStart = System.nanoTime();
        List<Integer> out = serverSearch(tdKw, tdRanges, queryKeywords);
        long serverEnd = System.nanoTime();

        clientSearchTimes.add((clientEnd - clientStart) / 1e6);
        serverSearchTimes.add((serverEnd - serverStart) / 1e6);
        return out;
    }

    private List<Integer> serverSearch(EASPEUtils.CipherVector tdKw, List<EASPEUtils.CipherVector> tdRanges, String[] queryKeywords) {
        if (queryKeywords == null || queryKeywords.length == 0) return Collections.emptyList();
        Map<Integer, Boolean> seen = new HashMap<>();
        List<Integer> results = new ArrayList<>();

        for (String w : queryKeywords) {
            if (w == null) continue;
            if (!index.containsKey(w)) continue;

            EASPEUtils.CipherVector encKwBasis = encKeywordBasis.get(w);
            if (encKwBasis == null) continue;
            double kwIP = EASPEUtils.innerProduct(encKwBasis, tdKw);
            if (Math.abs(kwIP) <= ZERO_EPS) {
                continue;
            }

            List<EncBlock> blocks = index.get(w);
            for (EncBlock block : blocks) {
                for (EncEntry e : block.entries) {
                    if (e.fileId < 0) continue; // phantom
                    boolean match = false;
                    for (EASPEUtils.CipherVector tdLoc : tdRanges) {
                        double locIP = EASPEUtils.innerProduct(e.encLoc, tdLoc);
                        if (Math.abs(locIP) <= ZERO_EPS) {
                            match = true;
                            break;
                        }
                    }
                    if (match && !seen.containsKey(e.fileId)) {
                        seen.put(e.fileId, Boolean.TRUE);
                        results.add(e.fileId);
                    }
                }
            }
        }
        return results;
    }

    private void updateInternal(int x, int y, String[] keywords, String op, int[] files, boolean measurePadding) {
        if (skKeyword == null || skLocation == null) {
            // lazy-init for dynamic-only paths
            this.skKeyword = EASPEUtils.keyGen(1, 1L);
            this.skLocation = EASPEUtils.keyGen(4 * t + 1, 2L);
            this.dictSize = 0;
        }
        if (!"add".equalsIgnoreCase(op)) {
            // EPSRQ baseline is static; for compatibility we ignore deletions.
            return;
        }
        int fileId = (files == null || files.length == 0) ? -1 : (files[0] % maxFiles);
        double[] loc = GrayCodeEncoder.encodePointLocationVector(x, y, t);
        EASPEUtils.CipherVector encLoc = EASPEUtils.encryptForIndex(skLocation, loc, rnd);

        if (keywords == null) return;
        for (String w : keywords) {
            if (w == null) continue;
            dict.computeIfAbsent(w, k -> dictSize++);

            // refresh keyword secret key if dict grew
            if (skKeyword.dim != Math.max(1, dictSize)) {
                // rebuild keyword key and cached basis ciphertexts (location key unchanged)
                this.skKeyword = EASPEUtils.keyGen(Math.max(1, dictSize), 1L);
                encKeywordBasis.clear();
            }
            encKeywordBasis.computeIfAbsent(w, kw -> {
                double[] basis = keywordBasisVector(kw);
                return EASPEUtils.encryptForIndex(skKeyword, basis, rnd);
            });

            List<EncBlock> blocks = index.computeIfAbsent(w, k -> new ArrayList<>());
            if (blocks.isEmpty() || blocks.get(blocks.size() - 1).entries.size() >= gamma) {
                blocks.add(new EncBlock());
            }
            blocks.get(blocks.size() - 1).entries.add(new EncEntry(fileId, encLoc));
            if (measurePadding) {
                ensurePadding(w);
            }
        }
    }

    private void ensurePadding(String w) {
        List<EncBlock> blocks = index.get(w);
        if (blocks == null || blocks.isEmpty()) return;
        EncBlock last = blocks.get(blocks.size() - 1);
        int real = last.entries.size();
        last.phantomCount = Math.max(0, gamma - real);
    }

    private double[] keywordBasisVector(String w) {
        int m = Math.max(1, dictSize);
        double[] v = new double[m];
        Integer idx = dict.get(w);
        if (idx != null && idx >= 0 && idx < m) {
            v[idx] = 1.0;
        }
        return v;
    }

    private double[] keywordQueryVector(String[] wq) {
        int m = Math.max(1, dictSize);
        double[] v = new double[m];
        if (wq == null) return v;
        for (String w : wq) {
            Integer idx = dict.get(w);
            if (idx != null && idx >= 0 && idx < m) {
                v[idx] += 1.0;
            }
        }
        return v;
    }

    public void clearUpdateTime() {
        totalUpdateTimes.clear();
    }

    public void clearSearchTime() {
        clientSearchTimes.clear();
        serverSearchTimes.clear();
    }

    public double getAverageUpdateTime() {
        return totalUpdateTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    public double getAverageSearchTime() {
        if (clientSearchTimes.size() != serverSearchTimes.size() || clientSearchTimes.isEmpty()) return 0.0;
        double sum = 0.0;
        for (int i = 0; i < clientSearchTimes.size(); i++) {
            sum += clientSearchTimes.get(i) + serverSearchTimes.get(i);
        }
        return sum / clientSearchTimes.size();
    }

    /**
     * A minimal row DTO to reuse existing dataset loaders in experiments.
     */
    public static final class Row {
        public final int fileId;
        public final int x;
        public final int y;
        public final String[] keywords;

        public Row(int fileId, int x, int y, String[] keywords) {
            this.fileId = fileId;
            this.x = x;
            this.y = y;
            this.keywords = keywords == null ? new String[0] : Arrays.copyOf(keywords, keywords.length);
        }
    }
}

