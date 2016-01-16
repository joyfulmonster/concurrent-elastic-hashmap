package org.joyfulmonster.util.concurrent.internal;

/**
 * It is intended to register different hash algorithms.
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
public class HashStrategy {
    public static HashStrategy getInstance() {
        return new WangJinkensHash();
    }

    public int hash(int code) {
        return code;
    }

    private static class WangJinkensHash extends HashStrategy{
        /** Copied from JDK ConcurrentHashMap implementation */
        /**
         * Applies a supplemental hash function to a given hashCode, which
         * defends against poor quality hash functions.  This is critical
         * because ConcurrentHashMap uses power-of-two length hash tables,
         * that otherwise encounter collisions for hashCodes that do not
         * differ in lower or upper bits.
         */
        @Override
        public int hash(int h) {
            // Spread bits to regularize both segment and index locations,
            // using variant of single-word Wang/Jenkins hash.
            h += (h <<  15) ^ 0xffffcd7d;
            h ^= (h >>> 10);
            h += (h <<   3);
            h ^= (h >>>  6);
            h += (h <<   2) + (h << 14);
            return h ^ (h >>> 16);
        }
    }
}
