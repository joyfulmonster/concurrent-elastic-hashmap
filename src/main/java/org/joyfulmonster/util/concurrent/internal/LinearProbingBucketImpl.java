package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of Bucket.
 *
 * 1. A bucket is a hashmap.
 * 2. this implementation uses Linear probing algorithm to resolve collision.
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
class LinearProbingBucketImpl<K, V> implements Bucket<K, V>, BucketMetricsSupport {

    /** localDepth of the bucket */
    private final int localDepth;
    /** the bucketID */
    private final int bucketID;
    /** The mask for calculating bucketIndex */
    private final int bucketIDMask;
    /** The max number of entries in this bucket */
    private final int bucketSize;
    /** The mask for the entry index, so the entry index will not overflow */
    private final int entryIndexMask;
    /** the upper limit of the entryCount before the bucket needs to be splitted */
    private final int loadThreshold;

    /** the nubmer of entries in this bucket.  EntryCount is only modified during a segment lock, so it's volatile instead of atom */
    private volatile int entryCount;
    /** the flag indicate whether this bucket is being split, and so this bucket will be abondoned */
    private volatile boolean valid;

    /** the lock coordination is done in the hashmap level, the entries are atomic array only because Java doesn't support arrays of volatile types.*/
    private final AtomicReferenceArray<HashEntry<K, V>> entries;
    /** Bucket level lock */
    private final ReentrantLock lock;
    /** The reference variable to the global entry count */
    private final AtomicInteger totalEntryCount;
    /** The matrix that track number of splits happened since map is created  */
    private final AtomicInteger totalSplitCount;
    /** The reference to the Directory object */
    private Directory directory;

    /**
     * Helper class to do linear probing collision resolution.
     */
    private class LinearProber {
        private int theIdx;
        private HashEntry entry;

        void probe (K key, int hashCode) {
            int slotIndex = findSlotIndex(hashCode);

            // linear probing resolve conflict
            theIdx = slotIndex;
            entry = entries.get(slotIndex);
            while (entry != null) {
                if (entry.getKeyHashCode() == hashCode && key.equals(entry.getKey())) {
                    break;
                } else {
                    theIdx++;
                    if (theIdx >= bucketSize) {
                        theIdx = 0;
                    } else if (theIdx == slotIndex) {
                        // it should not happen, since we use a threshold value to guide the bucket not too full.
                        throw new BucketOverflowError();
                    }

                    entry = entries.get(theIdx);
                }
            }
        }
    }

    /**
     *
     * @param localDepth   localDepth of the bucket
     * @param bucketID
     * @param bucketSize
     * @param loadFactor
     * @param totalEntryCount
     * @param totalSplitCount
     */
    LinearProbingBucketImpl(int localDepth, int bucketID, int bucketSize, float loadFactor,
                            AtomicInteger totalEntryCount, AtomicInteger totalSplitCount) {
        this.bucketSize = bucketSize;
        this.localDepth = localDepth;
        this.bucketID = bucketID;
        this.bucketIDMask = (1 << localDepth) - 1;
        this.entryIndexMask = bucketSize - 1;

        this.lock = new ReentrantLock(true);
        this.loadThreshold = (int) (((float) bucketSize) * loadFactor);

        this.entryCount = 0;
        this.valid = true;

        this.totalSplitCount = totalSplitCount;
        this.totalEntryCount = totalEntryCount;

        this.entries = new AtomicReferenceArray<HashEntry<K, V>>(bucketSize);
    }

    /**
     * Help method that links the LinearProbingBucketImpl back to the Directory
     *
     * @param directory
     */
    public LinearProbingBucketImpl directory(Directory directory) {
        this.directory = directory;
        return this;
    }

    /**
     * @inheritdoc
     */
    @Override
    public int getBucketIdx() {
        return this.bucketID;
    }

    /**
     * @inheritdoc
     */
    @Override
    public int getLocalDepth() {
        return this.localDepth;
    }

    /**
     * Put the entry into the bucket.
     * This method is running inside a lock.
     */
    @Override
    public V put(K key, V value, int hashCode, boolean replaceIfPresent) {
        return put(key, value, hashCode, replaceIfPresent, true);
    }

    /**
     * @inheritdoc
     */
    @Override
    public V transferEntry(K key, V value, int hashCode) {
        return put(key, value, hashCode, true, false);
    }

    /**
     * @inheritdoc
     */
    private V put(K key, V value, int hashCode, boolean replaceIfPresent, boolean countInTotalEntryCount) {
        LinearProber prober = new LinearProber();
        prober.probe(key, hashCode);

        V oldResult = null;
        if (prober.entry != null && prober.entry.getKeyHashCode() == hashCode && key.equals(prober.entry.getKey())) {
            oldResult = (V) prober.entry.getValue();
            if (replaceIfPresent) {
                HashEntry<K, V> newEntry = new HashEntry<K, V>(key, value, hashCode);
                entries.set(prober.theIdx, newEntry);
            }
        } else {
            // entry is null, so found the empty slot
            HashEntry<K, V> newEntry = new HashEntry<K, V>(key, value, hashCode);
            entries.set(prober.theIdx, newEntry);
            entryCount++;
            if (countInTotalEntryCount) {
                totalEntryCount.incrementAndGet();
            }
        }
        return oldResult;
    }

    /**
     * @inheritdoc
     */
    @Override
    public V remove(K key, int hashCode, V value) {
        LinearProber prober = new LinearProber();
        prober.probe(key, hashCode);

        V oldResult = null;
        if (prober.entry != null && prober.entry.getKeyHashCode() == hashCode && key.equals(prober.entry.getKey())) {
            oldResult = (V) prober.entry.getValue();
            boolean delete = true;
            if (value != null) {
                if (oldResult.equals(value)) {
                    delete = true;
                } else {
                    oldResult = null;
                    delete = false;
                }
            }

            if (delete) {
                entries.getAndSet(prober.theIdx, null);
                entryCount--;
                totalEntryCount.decrementAndGet();
            }
        }
        return oldResult;
    }

    /**
     * @inheritdoc
     */
    @Override
    public V get(K key, int hashCode) {
        LinearProber prober = new LinearProber();
        prober.probe(key, hashCode);

        V oldResult = null;
        if (prober.entry != null && prober.entry.getKeyHashCode() == hashCode && key.equals(prober.entry.getKey())) {
            oldResult = (V) prober.entry.getValue();
        }
        return oldResult;
    }

    /**
     * @inheritdoc
     */
    @Override
    public V splitAndPut(K key, V value, int hashCode, boolean replaceIfPresent) {
        /* this bucket will be abandoned, so it should not set the flag back to allow access */
        disallowAccess();
        totalSplitCount.incrementAndGet();

        int newLocalDepth = localDepth + 1;
        int newBucketID = 1 << localDepth;

        Bucket[] newBuckets = new Bucket[2];
        newBuckets[0] = directory.getBucketFactory().newBucket(newLocalDepth, bucketID);
        newBuckets[1] = directory.getBucketFactory().newBucket(newLocalDepth, bucketID | newBucketID);

        /** Spread the entries in this bucket to the new buckets */
        for (int i = 0; i < bucketSize; i++) {
            HashEntry<K, V> entry = entries.get(i);
            if (entry != null) {
                if (newBuckets[0].canHandle(entry.getKeyHashCode())) {
                    newBuckets[0].transferEntry(entry.getKey(), entry.getValue(), entry.getKeyHashCode());
                } else {
                    newBuckets[1].transferEntry(entry.getKey(), entry.getValue(), entry.getKeyHashCode());
                }
            }
        }

        /**
         * Now put the new entry into the two new buckets
         */
        V result;
        try {
            if (newBuckets[0].canHandle(hashCode)) {
                result = (V) newBuckets[0].put(key, value, hashCode, replaceIfPresent);
            } else if (newBuckets[1].canHandle(hashCode)) {
                result = (V) newBuckets[1].put(key, value, hashCode, replaceIfPresent);
            } else {
                // it should not happen
                throw new IllegalStateException("bucketIdxBits conflict during segment split");
            }
        } catch (BucketOverflowError soe1) {
            throw new IllegalStateException("sgement overflow occured after split");
        }
        directory.onSplit(this, newBuckets);
        return result;
    }

    /*
     * Implements replace(Object key, V value) and replace (Object key, V oldValue, V newValue).
     * If oldValue is null, treat it as replace(key, value), otherwise, only replace
     * if existing entry value equals oldValue.
     */
    @Override
    public V replace(K key, int hashCode, V oldValue, V newValue) {
        return put(key, newValue, hashCode, true);
    }

    /**
     * Check if this bucket still have enough space
     */
    public boolean hasMoreSpace() {
        return entryCount < loadThreshold;
    }

    /**
     * flag indicating if this bucket needs to be split, if yes, the bucket is not allow access
     */
    public boolean isInvalid() {
        return !valid;
    }

    /**
     * set the flag that the bucket needs to be splitted
     */
    public void disallowAccess() {
        valid = false;
    }

    /**
     * Lock the bucket. ReentrantLock is used, so the thread holding the lock can reenter.
     */
    @Override
    public void lock() {
        lock.lock();
    }

    /**
     * Unlock the bucket.
     */
    @Override
    public void unlock() {
        lock.unlock();
    }

    /**
     * All the entries in this bucket will have the hashcode that has the same collections of lower bits.
     */
    @Override
    public boolean canHandle(int hashCode) {
        return bucketIdxBits(hashCode) == bucketID;
    }

    /**
     * Map the hashCode to the entryIndex within the bucket.
     * @param hashCode
     * @return
     */
    private int findSlotIndex(int hashCode) {
        return (hashCode >>> localDepth) & entryIndexMask;
    }

    /**
     * Return the hashCode bucketIdx.
     * @param hashCode
     * @return
     */
    private int bucketIdxBits(int hashCode) {
        return hashCode & bucketIDMask;
    }

    /**
     * @inheritdoc
     */
    @Override
    public int getBucketEntries() {
        return entryCount;
    }
}
