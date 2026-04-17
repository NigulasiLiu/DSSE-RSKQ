package org.davidmoten.Scheme.EPSRQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * EPSRQ_Adapter:
 * - expose update/search methods compatible with current benchmark loops
 * - h -> T, l -> gamma mapping is applied by constructor arguments
 * - simulate update for static EPSRQ (append + padding)
 * - track nanoTime and trapdoor "communication size" (bytes)
 */
public final class EPSRQ_Adapter {

    private static final double ZERO_EPS = 1e-6;

    private final int t;
    private final int gamma;
    private final int maxFiles;
    private final Random rnd;

    private final EPSRQ_IndexBuilder builder;

    public final List<Double> totalUpdateTimes = new ArrayList<>();
    public final List<Double> clientSearchTimes = new ArrayList<>();
    public final List<Double> serverSearchTimes = new ArrayList<>();

    private long lastTrapdoorBytes = 0;

    public EPSRQ_Adapter(int maxFiles, int h, int lGamma, long seed) {
        this.t = h;
        this.gamma = Math.max(1, lGamma);
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(seed);
        this.builder = new EPSRQ_IndexBuilder(this.maxFiles, this.t, this.gamma, seed);
        this.builder.ensureSetup();
    }

    public long getLastTrapdoorBytes() {
        return lastTrapdoorBytes;
    }

    public double update(long[] pSet, String[] keywords, String op, int[] files) {
        long start = System.nanoTime();
        if ("add".equalsIgnoreCase(op)) {
            int fileId = (files == null || files.length == 0) ? -1 : files[0];
            builder.addRow(pSet[0], pSet[1], keywords, fileId);
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

        clientSearchTimes.add((clientEnd - clientStart) / 1e6);
        serverSearchTimes.add((serverEnd - serverStart) / 1e6);
        return res;
    }

    private List<Integer> serverSearch(EPSRQ_Setup.CipherVector tdText,
                                       List<EPSRQ_Setup.CipherVector> tdLocList,
                                       String[] queryKeywords) {
        if (queryKeywords == null || queryKeywords.length == 0) return new ArrayList<>();
        Map<String, List<EPSRQ_IndexBuilder.Block>> epki = builder.getEpki();

        Map<Integer, Boolean> seen = new HashMap<>();
        List<Integer> out = new ArrayList<>();
        EPSRQ_Setup s = new EPSRQ_Setup(0);

        for (String w : queryKeywords) {
            if (w == null) continue;
            List<EPSRQ_IndexBuilder.Block> blocks = epki.get(w);
            if (blocks == null) continue;

            EPSRQ_Setup.CipherVector encKwBasis = builder.getEncKeywordBasis(w);
            if (encKwBasis == null) continue;

            double kwIP = EPSRQ_Setup.innerProduct(encKwBasis, tdText);
            if (Math.abs(kwIP) <= ZERO_EPS) continue; // KeywordInnerProduct != 0 required

            for (EPSRQ_IndexBuilder.Block b : blocks) {
                for (EPSRQ_IndexBuilder.EncEntry e : b.entries) {
                    if (e.fileId < 0) continue; // phantom
                    boolean match = false;
                    for (EPSRQ_Setup.CipherVector tdLoc : tdLocList) {
                        double locIP = EPSRQ_Setup.innerProduct(e.encLoc, tdLoc);
                        if (Math.abs(locIP) <= ZERO_EPS) {
                            match = true;
                            break;
                        }
                    }
                    if (match && !seen.containsKey(e.fileId)) {
                        seen.put(e.fileId, Boolean.TRUE);
                        out.add(e.fileId);
                    }
                }
            }
        }
        return out;
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

