package org.davidmoten.Scheme.EPSRQ;

import org.davidmoten.Experiment.Comparison.FixRangeCompareToConstructionOne;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * EPSRQ_IndexBuilder:
 * - fixed text dimension m=8000 (global dictionary)
 * - space dimension = 4T+1 (+3 in EASPE key => 4T+4)
 * - offline build EPKI with strict gamma padding
 */
public final class EPSRQ_IndexBuilder {

    public static final class EncryptedLocation {
        public final int fileId; // -1 means phantom
        public final EPSRQ_Setup.CipherVector encLoc;
        public EncryptedLocation(int fileId, EPSRQ_Setup.CipherVector encLoc) {
            this.fileId = fileId;
            this.encLoc = encLoc;
        }
    }

    public static final class BuildStats {
        public final int dictionarySize;
        public final int realLocations;
        public final int phantomLocations;
        public final int keywordCount;
        public BuildStats(int dictionarySize, int realLocations, int phantomLocations, int keywordCount) {
            this.dictionarySize = dictionarySize;
            this.realLocations = realLocations;
            this.phantomLocations = phantomLocations;
            this.keywordCount = keywordCount;
        }
    }

    private static final int FIXED_TEXT_DIM = 8000;

    private final int t;
    private final int gamma;
    private final int maxFiles;
    private final Random rnd;

    private EPSRQ_Setup.SetupKeyPair keyPair;
    private final EPSRQ_Setup setup;

    private final Map<String, Integer> dict;
    private final List<String> dictionaryKeywords;
    private final Map<String, EPSRQ_Setup.CipherVector> encKeywordBasis;
    private final Map<String, List<EncryptedLocation>> epki;

    public EPSRQ_IndexBuilder(int maxFiles, int t, int gamma, long seed) {
        this.t = t;
        this.gamma = Math.max(1, gamma);
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(seed);
        this.dict = new HashMap<>(FIXED_TEXT_DIM * 2);
        this.dictionaryKeywords = new ArrayList<>(FIXED_TEXT_DIM);
        this.setup = new EPSRQ_Setup(seed);
        this.encKeywordBasis = new HashMap<>();
        this.epki = new HashMap<>();
    }

    public EPSRQ_Setup.SetupKeyPair getKeyPair() {
        if (keyPair == null) {
            this.keyPair = setup.setup(FIXED_TEXT_DIM, t);
        }
        return keyPair;
    }

    public Map<String, List<EncryptedLocation>> getEpki() {
        return epki;
    }

    public Map<String, EPSRQ_Setup.CipherVector> getEncKeywordBasis() {
        return encKeywordBasis;
    }

    public List<String> getDictionaryKeywords() {
        return dictionaryKeywords;
    }

    public BuildStats buildIndex(List<FixRangeCompareToConstructionOne.DataRow> allData) {
        dict.clear();
        dictionaryKeywords.clear();
        encKeywordBasis.clear();
        epki.clear();

        if (keyPair == null) {
            keyPair = setup.setup(FIXED_TEXT_DIM, t);
        }

        Set<String> unique = new TreeSet<>();
        if (allData != null) {
            for (FixRangeCompareToConstructionOne.DataRow row : allData) {
                if (row == null || row.keywords == null) {
                    continue;
                }
                for (String w : row.keywords) {
                    if (w != null && !w.isEmpty()) {
                        unique.add(w);
                    }
                }
            }
        }

        List<String> sorted = new ArrayList<>(unique);
        Collections.sort(sorted, Comparator.naturalOrder());
        int capped = Math.min(FIXED_TEXT_DIM, sorted.size());
        for (int i = 0; i < capped; i++) {
            String w = sorted.get(i);
            dict.put(w, i);
            dictionaryKeywords.add(w);
        }

        // If dataset has fewer than 8000 terms, reserve the rest as synthetic placeholders.
        for (int i = capped; i < FIXED_TEXT_DIM; i++) {
            String placeholder = "__epsrq_pad_kw_" + i;
            dict.put(placeholder, i);
            dictionaryKeywords.add(placeholder);
        }

        for (Map.Entry<String, Integer> e : dict.entrySet()) {
            encKeywordBasis.put(e.getKey(), encryptKeywordBasisByIndex(e.getValue()));
        }

        Map<String, List<int[]>> rawPosting = new LinkedHashMap<>(dict.size() * 2);
        for (String w : dictionaryKeywords) {
            rawPosting.put(w, new ArrayList<int[]>());
        }

        int realCount = 0;
        if (allData != null) {
            for (FixRangeCompareToConstructionOne.DataRow row : allData) {
                if (row == null || row.keywords == null) {
                    continue;
                }
                int fid = normalizeFileId(row.fileID);
                int x = (int) row.pointX;
                int y = (int) row.pointY;
                for (String w : row.keywords) {
                    if (w == null || w.isEmpty()) {
                        continue;
                    }
                    if (!dict.containsKey(w)) {
                        continue;
                    }
                    rawPosting.get(w).add(new int[]{fid, x, y});
                    realCount++;
                }
            }
        }

        int phantomCount = 0;
        for (String w : dictionaryKeywords) {
            List<int[]> postings = rawPosting.get(w);
            if (postings == null) {
                postings = new ArrayList<int[]>();
            }
            List<EncryptedLocation> out = new ArrayList<EncryptedLocation>(alignToGamma(postings.size()));
            for (int[] item : postings) {
                double[] loc = GrayCodeEncoder.encodePointLocationVector(item[1], item[2], t);
                EPSRQ_Setup.CipherVector enc = setup.encryptIndex(keyPair.kv2Space, loc, rnd);
                out.add(new EncryptedLocation(item[0], enc));
            }
            int missing = alignToGamma(postings.size()) - postings.size();
            for (int i = 0; i < missing; i++) {
                double[] phantom = GrayCodeEncoder.randomPhantomLocationVector(t, rnd);
                EPSRQ_Setup.CipherVector enc = setup.encryptIndex(keyPair.kv2Space, phantom, rnd);
                out.add(new EncryptedLocation(-1, enc));
                phantomCount++;
            }
            epki.put(w, out);
        }

        return new BuildStats(FIXED_TEXT_DIM, realCount, phantomCount, dictionaryKeywords.size());
    }

    public double[] keywordQueryVectorOr(String[] queryKeywords) {
        double[] q = new double[FIXED_TEXT_DIM];
        if (queryKeywords == null) {
            return q;
        }
        for (String w : queryKeywords) {
            Integer idx = dict.get(w);
            if (idx != null && idx >= 0 && idx < FIXED_TEXT_DIM) {
                q[idx] = 1.0;
            }
        }
        return q;
    }

    private EPSRQ_Setup.CipherVector encryptKeywordBasisByIndex(int idx) {
        double[] base = new double[FIXED_TEXT_DIM];
        base[idx] = 1.0;
        return setup.encryptIndex(keyPair.kv1Text, base, rnd);
    }

    private int alignToGamma(int size) {
        if (size <= 0) {
            return gamma;
        }
        int mod = size % gamma;
        return mod == 0 ? size : (size + (gamma - mod));
    }

    private int normalizeFileId(int fid) {
        int v = fid % maxFiles;
        return v < 0 ? v + maxFiles : v;
    }
}

