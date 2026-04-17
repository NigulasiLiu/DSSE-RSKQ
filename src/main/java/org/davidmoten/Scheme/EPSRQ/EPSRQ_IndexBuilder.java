package org.davidmoten.Scheme.EPSRQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * EPSRQ_IndexBuilder:
 * - vectorize (text + location)
 * - build EPKI (equal partition inverted index) with block size gamma
 * - generate phantom vectors for padding and encrypt them
 *
 * Memory note:
 * - store ciphertext as primitive double[] only
 * - pad only the tail block per keyword eagerly (as required by prompt)
 */
public final class EPSRQ_IndexBuilder {

    public static final class UpdateStats {
        public final int realEntriesAdded;
        public final int phantomsAdded;
        public UpdateStats(int realEntriesAdded, int phantomsAdded) {
            this.realEntriesAdded = realEntriesAdded;
            this.phantomsAdded = phantomsAdded;
        }
    }

    public static final class EncEntry {
        public final int fileId; // -1 for phantom
        public final EPSRQ_Setup.CipherVector encLoc;
        public EncEntry(int fileId, EPSRQ_Setup.CipherVector encLoc) {
            this.fileId = fileId;
            this.encLoc = encLoc;
        }
    }

    public static final class Block {
        public final List<EncEntry> entries;
        public Block(int gamma) {
            this.entries = new ArrayList<>(gamma);
        }
    }

    private final int t;
    private final int gamma;
    private final int maxFiles;
    private final Random rnd;

    private final Map<String, Integer> dict;
    private int dictSize;

    private EPSRQ_Setup.SetupKeyPair keyPair;
    private final EPSRQ_Setup setup;

    // encrypted keyword basis for EPKI keyword check
    private final Map<String, EPSRQ_Setup.CipherVector> encKeywordBasis;

    // EPKI: keyword -> blocks
    private final Map<String, List<Block>> epki;

    public EPSRQ_IndexBuilder(int maxFiles, int t, int gamma, long seed) {
        this.t = t;
        this.gamma = Math.max(1, gamma);
        this.maxFiles = Math.max(1, maxFiles);
        this.rnd = new Random(seed);
        this.dict = new HashMap<>();
        this.dictSize = 0;
        this.setup = new EPSRQ_Setup(seed);
        this.encKeywordBasis = new HashMap<>();
        this.epki = new HashMap<>();
    }

    public void ensureSetup() {
        if (keyPair == null) {
            // dictSize may still grow; kv1 will be regenerated when dict grows
            this.keyPair = setup.setup(Math.max(1, dictSize), t);
        }
    }

    public EPSRQ_Setup.SetupKeyPair getKeyPair() {
        ensureSetup();
        return keyPair;
    }

    public Map<String, List<Block>> getEpki() {
        return epki;
    }

    public EPSRQ_Setup.CipherVector getEncKeywordBasis(String w) {
        return encKeywordBasis.get(w);
    }

    public int getDictSize() {
        return Math.max(1, dictSize);
    }

    public UpdateStats addRow(long x, long y, String[] keywords, int fileId) {
        if (keywords == null) return new UpdateStats(0, 0);
        int fx = (int) x;
        int fy = (int) y;
        int fid = fileId % maxFiles;

        int realAdded = 0;
        int phantomAdded = 0;

        for (String w : keywords) {
            if (w == null) continue;
            int before = dictSize;
            dict.computeIfAbsent(w, k -> dictSize++);
            if (dictSize != before) {
                // dict grew => regenerate kv1 text keys as per (m+3)x(m+3)
                this.keyPair = setup.setup(Math.max(1, dictSize), t);
                encKeywordBasis.clear();
            }
            ensureSetup();

            encKeywordBasis.computeIfAbsent(w, kw -> encryptKeywordBasis(kw));

            double[] locPlain = GrayCodeEncoder.encodePointLocationVector(fx, fy, t); // (4T+1)
            EPSRQ_Setup.CipherVector encLoc = setup.encryptIndex(keyPair.kv2Space, locPlain, rnd);

            appendEntry(w, new EncEntry(fid, encLoc));
            realAdded++;
            phantomAdded += padTailBlockIfNeeded(w);
        }
        return new UpdateStats(realAdded, phantomAdded);
    }

    public int padTailBlockIfNeeded(String w) {
        List<Block> blocks = epki.get(w);
        if (blocks == null || blocks.isEmpty()) return 0;
        Block last = blocks.get(blocks.size() - 1);
        int added = 0;
        while (last.entries.size() < gamma) {
            double[] phantom = GrayCodeEncoder.randomPhantomLocationVector(t, rnd);
            EPSRQ_Setup.CipherVector encLoc = setup.encryptIndex(keyPair.kv2Space, phantom, rnd);
            last.entries.add(new EncEntry(-1, encLoc));
            added++;
        }
        return added;
    }

    private void appendEntry(String w, EncEntry e) {
        List<Block> blocks = epki.computeIfAbsent(w, k -> new ArrayList<>());
        if (blocks.isEmpty() || blocks.get(blocks.size() - 1).entries.size() >= gamma) {
            blocks.add(new Block(gamma));
        }
        blocks.get(blocks.size() - 1).entries.add(e);
    }

    private EPSRQ_Setup.CipherVector encryptKeywordBasis(String w) {
        // basis vector in m dims
        int m = Math.max(1, dictSize);
        double[] base = new double[m];
        Integer idx = dict.get(w);
        if (idx != null && idx >= 0 && idx < m) base[idx] = 1.0;

        return setup.encryptIndex(keyPair.kv1Text, base, rnd);
    }

    public double[] keywordQueryVectorOr(String[] queryKeywords) {
        int m = Math.max(1, dictSize);
        double[] q = new double[m];
        if (queryKeywords == null) return q;
        for (String w : queryKeywords) {
            Integer idx = dict.get(w);
            if (idx != null && idx >= 0 && idx < m) q[idx] = 1.0; // bitwise OR
        }
        return q;
    }

    // randoms are embedded in EASPEKey (r1,r2,r3)
}

