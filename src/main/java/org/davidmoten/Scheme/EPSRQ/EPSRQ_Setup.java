package org.davidmoten.Scheme.EPSRQ;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularMatrixException;

import java.security.SecureRandom;
import java.util.Random;

/**
 * EPSRQ_Setup: generate two independent EASPE key systems kv1(text) and kv2(space).
 *
 * EASPE extension (must follow):
 * - extend original vector by +3 dimensions
 *   - index time fills r1,r2,r3
 *   - trapdoor time fills r4,r5,r6 with constraint r1*r4 + r2*r5 + r3*r6 = 0
 * - apply a random permutation pi before splitting
 * - index uses M^T, trapdoor uses M^{-1}
 */
public final class EPSRQ_Setup {

    public static final class CipherVector {
        public final double[] c1;
        public final double[] c2;
        public CipherVector(double[] c1, double[] c2) {
            this.c1 = c1;
            this.c2 = c2;
        }
    }

    public static final class EASPEKey {
        public final int dim;        // extended dimension
        public final int[] s;        // split vector
        public final int[] pi;       // permutation
        public final int[] piInv;    // inverse permutation
        public final double r1;
        public final double r2;
        public final double r3;
        public final RealMatrix m1;
        public final RealMatrix m2;
        public final RealMatrix m1T;
        public final RealMatrix m2T;
        public final RealMatrix m1Inv;
        public final RealMatrix m2Inv;

        private EASPEKey(int dim, int[] s, int[] pi, int[] piInv,
                         double r1, double r2, double r3,
                         RealMatrix m1, RealMatrix m2, RealMatrix m1Inv, RealMatrix m2Inv) {
            this.dim = dim;
            this.s = s;
            this.pi = pi;
            this.piInv = piInv;
            this.r1 = r1;
            this.r2 = r2;
            this.r3 = r3;
            this.m1 = m1;
            this.m2 = m2;
            this.m1T = m1.transpose();
            this.m2T = m2.transpose();
            this.m1Inv = m1Inv;
            this.m2Inv = m2Inv;
        }
    }

    public static final class SetupKeyPair {
        public final EASPEKey kv1Text;
        public final EASPEKey kv2Space;
        public SetupKeyPair(EASPEKey kv1Text, EASPEKey kv2Space) {
            this.kv1Text = kv1Text;
            this.kv2Space = kv2Space;
        }
    }

    private final Random rnd;

    public EPSRQ_Setup(long seed) {
        this.rnd = seed == 0 ? new SecureRandom() : new Random(seed);
    }

    public SetupKeyPair setup(int mDictSize, int t) {
        int dimText = Math.max(1, mDictSize) + 3;     // (m+3)
        int dimSpace = (4 * t + 1) + 3;              // (4T+4)
        return new SetupKeyPair(
                keyGen(dimText, rnd),
                keyGen(dimSpace, rnd)
        );
    }

    public CipherVector encryptIndex(EASPEKey k, double[] v, Random localRnd) {
        double[] ext = extendAndPermute(k, v, k.r1, k.r2, k.r3);
        Split sp = splitIndex(ext, k.s, localRnd);
        return new CipherVector(k.m1T.operate(sp.v1), k.m2T.operate(sp.v2));
    }

    public CipherVector encryptTrapdoor(EASPEKey k, double[] q, Random localRnd) {
        double[] r456 = chooseTrapdoorRandoms(k.r1, k.r2, k.r3, localRnd);
        double[] ext = extendAndPermute(k, q, r456[0], r456[1], r456[2]);
        Split sp = splitTrapdoor(ext, k.s, localRnd);
        return new CipherVector(k.m1Inv.operate(sp.v1), k.m2Inv.operate(sp.v2));
    }

    public static double innerProduct(CipherVector idx, CipherVector td) {
        return dot(idx.c1, td.c1) + dot(idx.c2, td.c2);
    }

    public static double[] chooseTrapdoorRandoms(double r1, double r2, double r3, Random rnd) {
        // choose r4,r5 uniformly, then solve r6 to satisfy constraint:
        // r1*r4 + r2*r5 + r3*r6 = 0  =>  r6 = -(r1*r4 + r2*r5)/r3
        double r4 = nonZeroSymmetric(rnd);
        double r5 = nonZeroSymmetric(rnd);
        double rr3 = (Math.abs(r3) < 1e-12) ? (r3 >= 0 ? 1.0 : -1.0) : r3;
        double r6 = -((r1 * r4) + (r2 * r5)) / rr3;
        return new double[]{r4, r5, r6};
    }

    private static double nonZeroSymmetric(Random rnd) {
        double x;
        do {
            x = rnd.nextDouble() * 2.0 - 1.0;
        } while (Math.abs(x) < 1e-9);
        return x;
    }

    private static final class Split {
        final double[] v1;
        final double[] v2;
        Split(double[] v1, double[] v2) {
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    private static Split splitIndex(double[] v, int[] s, Random rnd) {
        int d = v.length;
        double[] v1 = new double[d];
        double[] v2 = new double[d];
        for (int i = 0; i < d; i++) {
            if (s[i] == 0) {
                v1[i] = v[i];
                v2[i] = v[i];
            } else {
                double r = rnd.nextDouble() * 2.0 - 1.0;
                v1[i] = r;
                v2[i] = v[i] - r;
            }
        }
        return new Split(v1, v2);
    }

    private static Split splitTrapdoor(double[] q, int[] s, Random rnd) {
        int d = q.length;
        double[] q1 = new double[d];
        double[] q2 = new double[d];
        for (int i = 0; i < d; i++) {
            if (s[i] == 0) {
                double r = rnd.nextDouble() * 2.0 - 1.0;
                q1[i] = r;
                q2[i] = q[i] - r;
            } else {
                q1[i] = q[i];
                q2[i] = q[i];
            }
        }
        return new Split(q1, q2);
    }

    private static double[] extendAndPermute(EASPEKey k, double[] base, double a, double b, double c) {
        int d0 = base.length;
        int d = k.dim;
        if (d0 + 3 != d) throw new IllegalArgumentException("base dim mismatch: " + d0 + " +3 != " + d);
        double[] ext = new double[d];
        System.arraycopy(base, 0, ext, 0, d0);
        ext[d0] = a;
        ext[d0 + 1] = b;
        ext[d0 + 2] = c;
        double[] perm = new double[d];
        for (int i = 0; i < d; i++) {
            perm[i] = ext[k.pi[i]];
        }
        return perm;
    }

    private static EASPEKey keyGen(int dim, Random rnd) {
        int[] s = new int[dim];
        for (int i = 0; i < dim; i++) s[i] = rnd.nextBoolean() ? 1 : 0;
        int[] pi = randomPermutation(dim, rnd);
        int[] piInv = invertPermutation(pi);
        double r1 = nonZeroSymmetric(rnd);
        double r2 = nonZeroSymmetric(rnd);
        double r3 = nonZeroSymmetric(rnd);
        RealMatrix m1 = randomInvertibleMatrix(dim, rnd);
        RealMatrix m2 = randomInvertibleMatrix(dim, rnd);
        RealMatrix m1Inv = invert(m1);
        RealMatrix m2Inv = invert(m2);
        return new EASPEKey(dim, s, pi, piInv, r1, r2, r3, m1, m2, m1Inv, m2Inv);
    }

    private static int[] randomPermutation(int n, Random rnd) {
        int[] p = new int[n];
        for (int i = 0; i < n; i++) p[i] = i;
        for (int i = n - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = p[i];
            p[i] = p[j];
            p[j] = tmp;
        }
        return p;
    }

    private static int[] invertPermutation(int[] p) {
        int[] inv = new int[p.length];
        for (int i = 0; i < p.length; i++) inv[p[i]] = i;
        return inv;
    }

    private static RealMatrix randomInvertibleMatrix(int dim, Random rnd) {
        // make diagonally-dominant matrix to reduce singular probability
        while (true) {
            double[][] a = new double[dim][dim];
            for (int i = 0; i < dim; i++) {
                double rowAbs = 0.0;
                for (int j = 0; j < dim; j++) {
                    double v = (rnd.nextInt(7) - 3); // [-3,3]
                    a[i][j] = v;
                    rowAbs += Math.abs(v);
                }
                a[i][i] += rowAbs + 1.0;
            }
            RealMatrix m = MatrixUtils.createRealMatrix(a);
            try {
                invert(m);
                return m;
            } catch (SingularMatrixException e) {
                // retry
            }
        }
    }

    private static RealMatrix invert(RealMatrix m) {
        return new LUDecomposition(m).getSolver().getInverse();
    }

    private static double dot(double[] a, double[] b) {
        double s = 0.0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }
}

