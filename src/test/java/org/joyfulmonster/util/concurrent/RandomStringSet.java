package org.joyfulmonster.util.concurrent;

import java.util.Random;

/**
 * Created by Weifeng Bao on 1/9/2016.
 */
public class RandomStringSet {
    private static final char[] symbols;

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    private final Random random;
    private final char[] singleStrBuf;

    private int count;
    private String[] data;

    public RandomStringSet(int count, int length, long salt) {
        if (length < 1) {
            throw new IllegalArgumentException("length < 1: " + length);
        }

        this.singleStrBuf = new char[length];
        this.count = count;
        this.random = new Random(salt);

        this.data = new String[count];
        for (int i = 0; i < count; i++) {
            data[i] = nextString();
        }
    }

    public String nextString() {
        for (int idx = 0; idx < singleStrBuf.length; ++idx) {
            singleStrBuf[idx] = symbols[random.nextInt(symbols.length)];
        }
        return new String(singleStrBuf);
    }

    public int size() {
        return count;
    }

    public String get(int j) {
        return data[j];
    }
}
