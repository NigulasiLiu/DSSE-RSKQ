package org.davidmoten.Experiment.EPSRQTest;

import org.davidmoten.Scheme.EPSRQ.EPSRQ_Adapter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Small correctness test:
 * - generate a small dataset with keywords
 * - run EPSRQ update to build index
 * - compare EPSRQ search results with plaintext filtering
 */
public final class EPSRQCorrectnessSmallTest {

    private static final class Obj {
        final int id;
        final int x;
        final int y;
        final String[] kws;
        Obj(int id, int x, int y, String[] kws) {
            this.id = id;
            this.x = x;
            this.y = y;
            this.kws = kws;
        }
    }

    public static void main(String[] args) throws Exception {
        int h = 8;                 // T
        int edge = 1 << h;
        int gamma = 1 << 12;       // small-ish
        int n = 2000;
        int kwUniverse = 200;
        int kwsPerObj = 12;
        long seed = 7L;

        Random rnd = new Random(seed);
        List<Obj> objs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int x = (int) Math.max(0, Math.min(edge - 1, rnd.nextGaussian() * (edge / 6.0) + edge / 2.0));
            int y = (int) Math.max(0, Math.min(edge - 1, rnd.nextGaussian() * (edge / 6.0) + edge / 2.0));
            String[] kws = randomDistinctKeywords(rnd, kwUniverse, kwsPerObj);
            objs.add(new Obj(i, x, y, kws));
        }

        EPSRQ_Adapter epsrq = new EPSRQ_Adapter(1 << 20, h, gamma, 2026L);
        for (Obj o : objs) {
            epsrq.update(new long[]{o.x, o.y}, o.kws, "add", new int[]{o.id});
        }

        // run 50 random queries
        for (int qi = 0; qi < 50; qi++) {
            int rPercent = 2 + rnd.nextInt(10); // 2%..11%
            int rangeLen = Math.max(1, edge * rPercent / 100);
            int x0 = rnd.nextInt(Math.max(1, edge - rangeLen));
            int y0 = rnd.nextInt(Math.max(1, edge - rangeLen));

            String[] queryKw = randomDistinctKeywords(rnd, kwUniverse, 4);

            Set<Integer> plain = plaintextFilter(objs, x0, y0, rangeLen, queryKw);
            Set<Integer> got = new HashSet<>(epsrq.searchRect(x0, y0, rangeLen, queryKw));

            // EPSRQ is a baseline scheme; correctness expectation here is "no false negatives" for our encoding design.
            // If you want strict equality, change this to plain.equals(got).
            if (!got.containsAll(plain)) {
                throw new IllegalStateException("Correctness failed at query " + qi +
                        ", missing=" + diff(plain, got).size());
            }
        }

        System.out.println("EPSRQ correctness small test passed.");
    }

    private static Set<Integer> plaintextFilter(List<Obj> objs, int x0, int y0, int len, String[] qkws) {
        int x1 = x0 + len;
        int y1 = y0 + len;
        Set<String> q = new HashSet<>();
        for (String s : qkws) q.add(s);

        Set<Integer> out = new HashSet<>();
        for (Obj o : objs) {
            if (o.x < x0 || o.x > x1 || o.y < y0 || o.y > y1) continue;
            boolean kwMatch = false;
            for (String k : o.kws) {
                if (q.contains(k)) {
                    kwMatch = true;
                    break;
                }
            }
            if (kwMatch) out.add(o.id);
        }
        return out;
    }

    private static Set<Integer> diff(Set<Integer> a, Set<Integer> b) {
        Set<Integer> d = new HashSet<>(a);
        d.removeAll(b);
        return d;
    }

    private static String[] randomDistinctKeywords(Random rnd, int W, int k) {
        Set<String> s = new HashSet<>();
        while (s.size() < k) {
            int idx = 1 + rnd.nextInt(W);
            s.add("k" + idx);
        }
        return s.toArray(new String[0]);
    }
}

