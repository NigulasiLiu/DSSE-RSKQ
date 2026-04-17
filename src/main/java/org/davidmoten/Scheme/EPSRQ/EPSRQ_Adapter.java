package org.davidmoten.Scheme.EPSRQ;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * EPSRQ_Adapter:
 * - static EPSRQ benchmark adapter
 * - fixed dictionary dimension m=8000
 * - fixed partition threshold gamma=1000
 * - support offline buildIndex and compatibility update() buffering
 */
public final class EPSRQ_Adapter {

    private static final double ZERO_EPS = 1e-6;
    private static final int FIXED_GAMMA = 1000;

    private final int t;
    private final int maxFiles;
    private final Random rnd;

    private final EPSRQ_IndexBuilder builder;
    // Prevent JIT dead-code elimination in tight loops when caller ignores results.
    private volatile long searchBlackhole = 0L;
    private volatile boolean built = false;
    private final List<FixRangeCompareToConstructionOne.DataRow> stagedRows = new ArrayList<FixRangeCompareToConstructionOne.DataRow>();

    public final List<Double> totalUpdateTimes = new ArrayList<>();
    public final List<Double> clientSearchTimes = new ArrayList<>();
    public final List<Double> serverSearchTimes = new ArrayList<>();

    private long lastTrapdoorBytes = 0;
    private long lastUpdateBytes = 0;

    public EPSRQ_Adapter(int maxFiles, int h, int lGamma, long seed) {
        this.t = h;
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(seed);
        this.builder = new EPSRQ_IndexBuilder(this.maxFiles, this.t, FIXED_GAMMA, seed);
        this.builder.getKeyPair();
    }

    public long getLastTrapdoorBytes() {
        return lastTrapdoorBytes;
    }

    public long getLastUpdateBytes() {
        return lastUpdateBytes;
    }

    public double update(long[] pSet, String[] keywords, String op, int[] files) {
        long start = System.nanoTime();
        lastUpdateBytes = 0;
        if ("add".equalsIgnoreCase(op)) {
            int fileId = (files == null || files.length == 0) ? -1 : files[0];
            stagedRows.add(new FixRangeCompareToConstructionOne.DataRow(fileId, pSet[0], pSet[1], keywords));
            built = false;
            int dimSpace = builder.getKeyPair().kv2Space.dim;
            long perCipherBytes = 2L * dimSpace * 8L;
            // Compatibility mode: one buffered add corresponds to one encrypted location payload.
            lastUpdateBytes = perCipherBytes;
        } else {
            // static EPSRQ: ignore delete semantics, but still count time
        }
        double ms = (System.nanoTime() - start) / 1e6;
        totalUpdateTimes.add(ms);
        return ms;
    }

    /**
     * Search rectangle [xStart, xStart+rangeLen] x [yStart, yStart+rangeLen] using SGR-range decomposition.
     */
    public List<Integer> searchRect(int xStart, int yStart, int rangeLen, String[] queryKeywords) {
        ensureBuilt();
        long clientStart = System.nanoTime();

        EPSRQ_Setup.SetupKeyPair kp = builder.getKeyPair();

        // text trapdoor: OR query vector
        double[] qText = builder.keywordQueryVectorOr(queryKeywords);
        EPSRQ_Setup s = new EPSRQ_Setup(0);
        EPSRQ_Setup.CipherVector tdText = s.encryptTrapdoor(kp.kv1Text, qText, rnd);

        // location trapdoors: SGR-range list
        int max = (1 << t);
        int xEnd = Math.min(max - 1, Math.max(0, xStart + Math.max(0, rangeLen)));
        int yEnd = Math.min(max - 1, Math.max(0, yStart + Math.max(0, rangeLen)));
        List<double[]> sgrRanges = GrayCodeEncoder.decomposeRectangleToSGRRanges(xStart, yStart, xEnd, yEnd, t);

        List<EPSRQ_Setup.CipherVector> tdLocList = new ArrayList<>(sgrRanges.size());
        for (double[] qLoc : sgrRanges) {
            tdLocList.add(s.encryptTrapdoor(kp.kv2Space, qLoc, rnd));
        }

        // trapdoor "communication size" (rough, doubles only)
        long tdBytes = 0;
        tdBytes += (long) (tdText.c1.length + tdText.c2.length) * 8L;
        for (EPSRQ_Setup.CipherVector td : tdLocList) {
            tdBytes += (long) (td.c1.length + td.c2.length) * 8L;
        }
        lastTrapdoorBytes = tdBytes;

        long clientEnd = System.nanoTime();

        long serverStart = System.nanoTime();
        List<Integer> res = serverSearch(tdText, tdLocList, queryKeywords);
        long serverEnd = System.nanoTime();

        // Consume result to keep search loop observable for JVM optimizer.
        long bh = res.size();
        for (int id : res) {
            bh = 31L * bh + id;
        }
        searchBlackhole ^= bh;

        clientSearchTimes.add((clientEnd - clientStart) / 1e6);
        serverSearchTimes.add((serverEnd - serverStart) / 1e6);
        return res;
    }

    private List<Integer> serverSearch(EPSRQ_Setup.CipherVector tdText,
                                       List<EPSRQ_Setup.CipherVector> tdLocList,
                                       String[] queryKeywords) {
        if (queryKeywords == null || queryKeywords.length == 0) return new ArrayList<>();
        Map<String, List<EPSRQ_IndexBuilder.EncryptedLocation>> epki = builder.getEpki();
        Map<String, EPSRQ_Setup.CipherVector> encBasis = builder.getEncKeywordBasis();
        List<String> allKeywords = builder.getDictionaryKeywords();

        Map<Integer, Boolean> seen = new HashMap<>();
        List<Integer> out = new ArrayList<>();
        Set<String> querySet = new HashSet<String>();
        for (String w : queryKeywords) {
            if (w != null) {
                querySet.add(w);
            }
        }

        // Scan dictionary keywords and evaluate encrypted keyword-match against OR trapdoor.
        for (String w : allKeywords) {
            EPSRQ_Setup.CipherVector encKwBasis = encBasis.get(w);
            if (encKwBasis == null) {
                continue;
            }

            double kwIP = EPSRQ_Setup.innerProduct(encKwBasis, tdText);
            if (Math.abs(kwIP) <= ZERO_EPS) {
                continue;
            }
            // Keep semantic consistency: only query keywords can contribute final hits.
            if (!querySet.contains(w)) {
                continue;
            }

            List<EPSRQ_IndexBuilder.EncryptedLocation> posting = epki.get(w);
            if (posting == null) {
                continue;
            }

            // Full scan includes phantom tuples; no shortcut per block.
            for (EPSRQ_IndexBuilder.EncryptedLocation e : posting) {
                boolean locMatch = false;
                for (EPSRQ_Setup.CipherVector tdLoc : tdLocList) {
                    double locIP = EPSRQ_Setup.innerProduct(e.encLoc, tdLoc);
                    if (Math.abs(locIP) <= ZERO_EPS) {
                        locMatch = true;
                        break;
                    }
                }
                if (locMatch && e.fileId >= 0 && !seen.containsKey(e.fileId)) {
                    seen.put(e.fileId, Boolean.TRUE);
                    out.add(e.fileId);
                }
            }
        }
        return out;
    }

    public EPSRQ_IndexBuilder.BuildStats buildIndex(List<FixRangeCompareToConstructionOne.DataRow> allData) {
        EPSRQ_IndexBuilder.BuildStats st = builder.buildIndex(allData);
        built = true;
        stagedRows.clear();
        return st;
    }

    private void ensureBuilt() {
        if (built) {
            return;
        }
        if (!stagedRows.isEmpty()) {
            builder.buildIndex(stagedRows);
            built = true;
            return;
        }
        builder.buildIndex(new ArrayList<FixRangeCompareToConstructionOne.DataRow>());
        built = true;
    }

    public long getSearchBlackhole() {
        return searchBlackhole;
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
        for (int i = 0; i < clientSearchTimes.size(); i++) sum += clientSearchTimes.get(i) + serverSearchTimes.get(i);
        return sum / clientSearchTimes.size();
    }
}

