package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Weifeng Bao on 1/15/2016.
 */

class DirectoryImpl extends AtomicReference<AtomicReferenceArray<Bucket>> implements Directory {
    /**
     * Locker for Directory instance
     */
    private final ReentrantLock lock;
    /**
     * The number of buckets
     */
    private volatile int bucketCount;
    /**
     * bucket factory instance
     */
    private BucketFactory bucketFactory;

    /**
     * Private constructor, should not be called directly, only be called from create() method
     *
     * @param buckets
     */
    DirectoryImpl(AtomicReferenceArray<Bucket> buckets) {
        super(buckets);
        lock = new ReentrantLock(true);
        bucketCount = buckets.length();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public int getBucketCount() {
        return bucketCount;
    }

    /**
     * The lower bits of the hashCode are used as the bucket index.
     *
     * @param hashCode
     * @return
     */
    public Bucket getBucket(int hashCode) {
        AtomicReferenceArray<Bucket> dir = get();
        int dirSize = dir.length();
        int dirMask = dirSize - 1;
        int bucketIndex = hashCode & dirMask;
        Bucket bucket = dir.get(bucketIndex);

        return bucket;
    }

    /**
     * Wire in the BucketFactory instance.
     *
     * @param factory
     * @return
     */
    Directory bucketFactory(BucketFactory factory) {
        bucketFactory = factory;
        return this;
    }

    /**
     * Return bucketFactory instance.
     *
     * @return
     */
    public BucketFactory getBucketFactory() {
        return bucketFactory;
    }

    /**
     * Take care directory with the two newly created buckets.
     * <p/>
     * 1. if the newly created buckets local depth is smaller than directory global depth, there is no need to double directory
     * 2. otherwise, double the directory size, and rewire all the directory -> buckets mapping.
     *
     * @param newBuckets
     * @return
     */
    public int onSplit(Bucket oldBucket, Bucket[] newBuckets) {
        lock();
        try {
            this.bucketCount++; // doesn't need to be atomic; only modified under directory lock
            AtomicReferenceArray<Bucket> bucketArray = get();
            int bucketsNumber = bucketArray.length();
            int dirMask = bucketsNumber - 1;
            int globalDepth = Integer.bitCount(dirMask);

            if (globalDepth < newBuckets[0].getLocalDepth()) {
				/*
				 * double directory size
				 */
                int newDirSize = bucketsNumber * 2;
                if (newDirSize > ConcurrentElasticHashMapImpl.MAX_BUCKET_COUNT) {
                    throw new IllegalStateException("directory size limit exceeded");
                }

                /**
                 * Link the old existing buckets
                 */
                AtomicReferenceArray<Bucket> newDirectory = new AtomicReferenceArray<Bucket>(newDirSize);
                for (int i = 0; i < bucketsNumber; i++) {
                    newDirectory.set(i, bucketArray.get(i));
                    newDirectory.set(i + bucketsNumber, bucketArray.get(i));
                }

                /**
                 * Set the Directory's bucketArray pointer to new Directory.
                 */
                bucketArray = newDirectory;
                bucketsNumber = newDirSize;
                set(newDirectory);
            } else {
                // otherwise, the directory do not need to be doubled.
            }

            /**
             * wire up the new two buckets
             */
            final int step = 1 << newBuckets[0].getLocalDepth();
            for (int i = newBuckets[1].getBucketIdx(); i < bucketsNumber; i += step) {
                bucketArray.set(i, newBuckets[1]);
            }
            for (int i = newBuckets[0].getBucketIdx(); i < bucketsNumber; i += step) {
                bucketArray.set(i, newBuckets[0]);
            }
        } finally {
            unlock();
        }

        return bucketCount;
    }

    @Override
    public int onMerge(Bucket oldBucket) {
        return 0;
    }
}
