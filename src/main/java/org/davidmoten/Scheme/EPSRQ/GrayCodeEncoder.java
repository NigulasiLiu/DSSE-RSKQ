package org.davidmoten.Scheme.EPSRQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * GrayCodeEncoder:
 * - point location vector: (4T+1), last = 2T
 * - SGR-range vector: (4T+1), last = -1
 *
 * First 4T dimensions are pairs for 2T gray bits:
 * - For a concrete bit b in {0,1}, encode as pair:
 *   b=0 -> (1,0), b=1 -> (0,1)  (i.e., "10" or "01")
 *
 * For range constraint per bit:
 * - fixed 0 -> (1,0)
 * - fixed 1 -> (0,1)
 * - wildcard * -> (1,1)  (always contributes 1 to dot product)
 *
 * With this design:
 * - for any point satisfying all fixed constraints, dot(range, point) over first 4T dims equals 2T,
 *   so LocationInnerProduct = 2T + (-1)*2T = 0
 * - if any fixed constraint mismatches, sum < 2T => inner product != 0
 */
public final class GrayCodeEncoder {

    private GrayCodeEncoder() {
    }

    public enum BitConstraint {
        ZERO, ONE, WILDCARD
    }

    public static int toGray(int x) {
        return x ^ (x >>> 1);
    }

    public static int grayBit(int grayValue, int posFromMsb, int bits) {
        int shift = bits - 1 - posFromMsb;
        return (grayValue >>> shift) & 1;
    }

    public static double[] encodePointLocationVector(int x, int y, int t) {
        int gx = toGray(x);
        int gy = toGray(y);
        int d = 4 * t + 1;
        double[] v = new double[d];
        int idx = 0;
        for (int i = 0; i < t; i++) {
            idx = putBitPair(v, idx, grayBit(gx, i, t));
        }
        for (int i = 0; i < t; i++) {
            idx = putBitPair(v, idx, grayBit(gy, i, t));
        }
        v[idx] = 2.0 * t;
        return v;
    }

    public static double[] encodeSgrRangeVector(BitConstraint[] xBits, BitConstraint[] yBits, int t) {
        if (xBits.length != t || yBits.length != t) {
            throw new IllegalArgumentException("constraint length must be t");
        }
        int d = 4 * t + 1;
        double[] q = new double[d];
        int idx = 0;
        for (int i = 0; i < t; i++) {
            idx = putConstraintPair(q, idx, xBits[i]);
        }
        for (int i = 0; i < t; i++) {
            idx = putConstraintPair(q, idx, yBits[i]);
        }
        q[idx] = -1.0;
        return q;
    }

    /**
     * Decompose axis interval [start, end] (inclusive) into dyadic blocks,
     * then convert each block into Gray-bit constraints.
     *
     * Note: this is a practical SGR-range approximation that avoids enumerating all cells.
     */
    public static List<BitConstraint[]> decompose1DToConstraints(int start, int end, int t) {
        if (start > end) return new ArrayList<>();
        int max = (1 << t) - 1;
        int l = Math.max(0, start);
        int r = Math.min(max, end);
        List<BitConstraint[]> out = new ArrayList<>();
        while (l <= r) {
            int maxPow = largestPowerOfTwoBlock(l, r);
            out.add(blockToGrayConstraints(l, maxPow, t));
            l += maxPow;
        }
        return out;
    }

    public static List<double[]> decomposeRectangleToSGRRanges(int xStart, int yStart, int xEnd, int yEnd, int t) {
        List<BitConstraint[]> xs = decompose1DToConstraints(xStart, xEnd, t);
        List<BitConstraint[]> ys = decompose1DToConstraints(yStart, yEnd, t);
        List<double[]> out = new ArrayList<>(Math.max(1, xs.size() * ys.size()));
        for (BitConstraint[] xc : xs) {
            for (BitConstraint[] yc : ys) {
                out.add(encodeSgrRangeVector(xc, yc, t));
            }
        }
        return out;
    }

    /**
     * Phantom vector rule:
     * - first 4T dims are random "10" or "01" pairs (valid-looking Gray pairs)
     * - last dim must != 2T to prevent accidental location match
     */
    public static double[] randomPhantomLocationVector(int t, Random rnd) {
        int d = 4 * t + 1;
        double[] v = new double[d];
        int idx = 0;
        for (int i = 0; i < 2 * t; i++) {
            boolean oneZero = rnd.nextBoolean();
            v[idx++] = oneZero ? 1.0 : 0.0;
            v[idx++] = oneZero ? 0.0 : 1.0;
        }
        double last = 2.0 * t;
        // force != 2T
        v[idx] = last + 1.0 + rnd.nextInt(3);
        return v;
    }

    private static int putBitPair(double[] v, int idx, int bit) {
        // bit 0 -> (1,0), bit 1 -> (0,1)
        if (bit == 0) {
            v[idx++] = 1.0;
            v[idx++] = 0.0;
        } else {
            v[idx++] = 0.0;
            v[idx++] = 1.0;
        }
        return idx;
    }

    private static int putConstraintPair(double[] v, int idx, BitConstraint c) {
        switch (c) {
            case ZERO:
                v[idx++] = 1.0;
                v[idx++] = 0.0;
                return idx;
            case ONE:
                v[idx++] = 0.0;
                v[idx++] = 1.0;
                return idx;
            case WILDCARD:
                v[idx++] = 1.0;
                v[idx++] = 1.0;
                return idx;
            default:
                throw new IllegalStateException("unexpected constraint");
        }
    }

    private static int largestPowerOfTwoBlock(int l, int r) {
        // largest power-of-two size aligned at l and not exceeding r
        int maxSize = 1;
        int remain = r - l + 1;
        // alignment: lowbit(l) in binary domain
        int lowbit = l & -l;
        if (lowbit == 0) lowbit = 1 << 30;
        maxSize = Math.min(lowbit, highestPowerOfTwoAtMost(remain));
        return Math.max(1, maxSize);
    }

    private static int highestPowerOfTwoAtMost(int x) {
        int p = 1;
        while ((p << 1) > 0 && (p << 1) <= x) p <<= 1;
        return p;
    }

    private static BitConstraint[] blockToGrayConstraints(int start, int size, int t) {
        // size is power of two, so lower log2(size) bits are wildcards in binary.
        int wildBits = Integer.numberOfTrailingZeros(size);
        if (wildBits > t) wildBits = t;
        int g = toGray(start);
        BitConstraint[] c = new BitConstraint[t];
        for (int i = 0; i < t; i++) {
            int fromLsb = t - 1 - i;
            if (fromLsb < wildBits) {
                c[i] = BitConstraint.WILDCARD;
            } else {
                int b = grayBit(g, i, t);
                c[i] = (b == 0) ? BitConstraint.ZERO : BitConstraint.ONE;
            }
        }
        return c;
    }
}

