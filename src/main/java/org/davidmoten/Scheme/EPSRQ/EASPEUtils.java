package org.davidmoten.Scheme.EPSRQ;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularMatrixException;

import java.security.SecureRandom;
import java.util.Random;

public final class EASPEUtils {

    private EASPEUtils() {
    }

    public static final class SecretKey {
        public final int dim;
        public final int[] s;
        public final RealMatrix m1;
        public final RealMatrix m2;
        public final RealMatrix m1Inv;
        public final RealMatrix m2Inv;
        public final RealMatrix m1T;
        public final RealMatrix m2T;

        private SecretKey(int dim, int[] s, RealMatrix m1, RealMatrix m2, RealMatrix m1Inv, RealMatrix m2Inv) {
            this.dim = dim;
            this.s = s;
            this.m1 = m1;
            this.m2 = m2;
            this.m1Inv = m1Inv;
            this.m2Inv = m2Inv;
            this.m1T = m1.transpose();
            this.m2T = m2.transpose();
        }
    }

    public static final class CipherVector {
        public final double[] c1;
        public final double[] c2;

        public CipherVector(double[] c1, double[] c2) {
            this.c1 = c1;
            this.c2 = c2;
        }
    }

    public static SecretKey keyGen(int dim, long seed) {
        Random rnd = (seed == 0) ? new SecureRandom() : new Random(seed);
        int[] s = new int[dim];
        for (int i = 0; i < dim; i++) {
            s[i] = rnd.nextBoolean() ? 1 : 0;
        }

        RealMatrix m1 = randomInvertibleMatrix(dim, rnd);
        RealMatrix m2 = randomInvertibleMatrix(dim, rnd);
        RealMatrix m1Inv = invert(m1);
        RealMatrix m2Inv = invert(m2);
        return new SecretKey(dim, s, m1, m2, m1Inv, m2Inv);
    }

    public static CipherVector encryptForIndex(SecretKey sk, double[] v, Random rnd) {
        Split sp = splitIndexVector(v, sk.s, rnd);
        double[] c1 = sk.m1T.operate(sp.v1);
        double[] c2 = sk.m2T.operate(sp.v2);
        return new CipherVector(c1, c2);
    }

    public static CipherVector trapdoor(SecretKey sk, double[] q, Random rnd) {
        Split sp = splitQueryVector(q, sk.s, rnd);
        double[] t1 = sk.m1Inv.operate(sp.v1);
        double[] t2 = sk.m2Inv.operate(sp.v2);
        return new CipherVector(t1, t2);
    }

    public static double innerProduct(CipherVector index, CipherVector trapdoor) {
        return dot(index.c1, trapdoor.c1) + dot(index.c2, trapdoor.c2);
    }

    private static final class Split {
        final double[] v1;
        final double[] v2;

        private Split(double[] v1, double[] v2) {
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    private static Split splitIndexVector(double[] v, int[] s, Random rnd) {
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

    private static Split splitQueryVector(double[] q, int[] s, Random rnd) {
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

    private static RealMatrix randomInvertibleMatrix(int dim, Random rnd) {
        while (true) {
            double[][] a = new double[dim][dim];
            for (int i = 0; i < dim; i++) {
                for (int j = 0; j < dim; j++) {
                    a[i][j] = rnd.nextInt(7) - 3; // [-3,3]
                }
                a[i][i] += 10.0; // bias towards invertible
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
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }
}

