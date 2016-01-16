package org.joyfulmonster.util.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wbao on 1/11/2016.
 */
class Directory extends AtomicReference<AtomicReferenceArray<Bucket>> {
    /**
     * Bootstrap Directory object:
     *
     * 1. create Bucket array based on the configuration parameter
     * 2. new Directory instance
     * 3. Link the Bucket to the Directory instance.
     * 4. create BucketFactory instance, and provisioning configuration parameter into the Factory.   Whenever somewhere
     *    need to create a new bucket, it should ask Directory for the BucketFactory reference and call newBucket from
     *    there.
     *
     * @param bucketCount
     * @param bucketSize
     * @param loadFactor
     * @param totalEntryCount
     * @param splitCount
     * @return
     */
    public static Directory create(int bucketCount, int bucketSize, float loadFactor, AtomicInteger totalEntryCount, AtomicInteger splitCount) {
        int dirSize = bucketCount;
        int dirMask = dirSize - 1;
        int globalDepth = Integer.bitCount(dirMask);

        AtomicReferenceArray<Bucket> bucketsArray = new AtomicReferenceArray<Bucket>(bucketCount);
        for (int bucketIdex = 0; bucketIdex < bucketCount; bucketIdex++) {
            bucketsArray.set(bucketIdex, new LinearProbingBucketImpl(globalDepth, bucketIdex, bucketSize, loadFactor, totalEntryCount, splitCount));
        }

        Directory result = new Directory(bucketsArray);
        for (int i = 0; i < bucketCount; i++) {
            ((LinearProbingBucketImpl)bucketsArray.get(i)).directory(result);
        }

        BucketFactory bucketFactory = new BucketFactory(result, bucketSize, loadFactor, totalEntryCount, splitCount);
        result.bucketFactory(bucketFactory);

        return result;
    }

    /** Locker for Directory instance */
    private final ReentrantLock lock;
    /** The number of buckets */
    private volatile int bucketCount;
    /** bucket factory instance */
    private BucketFactory bucketFactory;

    /**
     * Private constructor, should not be called directly, only be called from create() method
     * @param buckets
     */
    private Directory(AtomicReferenceArray<Bucket> buckets) {
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
     * @param factory
     * @return
     */
    private Directory bucketFactory(BucketFactory factory) {
        bucketFactory = factory;
        return this;
    }

    /**
     * Return bucketFactory instance.
     * @return
     */
    public BucketFactory getBucketFactory() {
        return bucketFactory;
    }

    /**
     * Take care directory with the two newly created buckets.
     *
     * 1. if the newly created buckets local depth is smaller than directory global depth, there is no need to double directory
     * 2. otherwise, double the directory size, and rewire all the directory -> buckets mapping.
     *
     * @param newBuckets
     * @return
     */
    public int onSplit(Bucket[] newBuckets) {
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
                if (newDirSize > ConcurrentExtendiableHashMapImpl.MAX_BUCKET_COUNT) {
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
            }
            else {
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
        }
        finally {
            unlock();
        }

        return bucketCount;
    }
}
