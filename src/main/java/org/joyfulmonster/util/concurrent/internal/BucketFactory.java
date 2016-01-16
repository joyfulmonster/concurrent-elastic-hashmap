package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provide the Factory facility to encapsulate the Bucket creation logic.
 *
 * A Directory maintains a instance of BucketFactory.  If there is a need to create a new Bucket, the code should go
 * to Directory to ask for the BucketFactory instance and call newBucket.
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
class BucketFactory {
    private Directory directory;
    private int bucketSize;
    private float bucketLoadFactor;
    private AtomicInteger totalEntryCount;
    private AtomicInteger totalSplitCount;

    BucketFactory(Directory directory, int bucketSize, float bucketLoadFactor, AtomicInteger totalEntryCount, AtomicInteger splitCount) {
        this.directory = directory;
        this.bucketLoadFactor = bucketLoadFactor;
        this.bucketSize = bucketSize;
        this.totalEntryCount = totalEntryCount;
        this.totalSplitCount = splitCount;
    }

    public Bucket newBucket(int localDepth, int bucketIdx) {
        Bucket result = new LinearProbingBucketImpl(localDepth, bucketIdx, bucketSize, bucketLoadFactor,  totalEntryCount, totalSplitCount).directory(directory);
        return result;
    }
}
