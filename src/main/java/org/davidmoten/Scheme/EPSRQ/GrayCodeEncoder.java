package org.davidmoten.Scheme.EPSRQ;

import java.util.Arrays;

public final class GrayCodeEncoder {

    private GrayCodeEncoder() {
    }

    public static int toGray(int binary) {
        return binary ^ (binary >>> 1);
    }

    public static int[] toGrayBits(int value, int bits) {
        int g = toGray(value);
        int[] out = new int[bits];
        for (int i = 0; i < bits; i++) {
            int shift = bits - 1 - i;
            out[i] = (g >>> shift) & 1;
        }
        return out;
    }

    public static double[] encodePointLocationVector(int x, int y, int t) {
        int[] gx = toGrayBits(x, t);
        int[] gy = toGrayBits(y, t);
        int d = 4 * t + 1;
        double[] v = new double[d];
        int idx = 0;
        for (int i = 0; i < t; i++) {
            v[idx++] = gx[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = 1 - gx[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = gy[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = 1 - gy[i];
        }
        v[idx] = 2.0 * t;
        return v;
    }

    public static double[] encodeCellRangeVector(int x, int y, int t) {
        int[] gx = toGrayBits(x, t);
        int[] gy = toGrayBits(y, t);
        int d = 4 * t + 1;
        double[] v = new double[d];
        int idx = 0;
        for (int i = 0; i < t; i++) {
            v[idx++] = gx[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = 1 - gx[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = gy[i];
        }
        for (int i = 0; i < t; i++) {
            v[idx++] = 1 - gy[i];
        }
        v[idx] = -1.0;
        return v;
    }

    public static double[] randomPhantomLocationVector(int t, java.util.Random rnd) {
        int max = (1 << t);
        int x = rnd.nextInt(max);
        int y = rnd.nextInt(max);
        return encodePointLocationVector(x, y, t);
    }

    public static double[] copy(double[] v) {
        return Arrays.copyOf(v, v.length);
    }
}

