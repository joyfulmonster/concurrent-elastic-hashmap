package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Weifeng Bao on 1/16/2016.
 */
class DirectoryFactory {
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

        DirectoryImpl result = new DirectoryImpl(bucketsArray);
        for (int i = 0; i < bucketCount; i++) {
            ((LinearProbingBucketImpl)bucketsArray.get(i)).directory(result);
        }

        BucketFactory bucketFactory = new BucketFactory(result, bucketSize, loadFactor, totalEntryCount, splitCount);
        result.bucketFactory(bucketFactory);

        return result;
    }
}
