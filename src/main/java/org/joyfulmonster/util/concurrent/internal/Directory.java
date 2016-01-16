package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by Weifeng Bao on 1/11/2016.
 */
interface Directory {
    /**
     * Lock directory
     */
    public void lock();

    /**
     * Unlock directory
     */
    public void unlock();

    /**
     * Get the bucket count
     * @return
     */
    public int getBucketCount();

    /**
     * Get the bucket for a hashcode
     * @param hashCode
     * @return
     */
    public Bucket getBucket(int hashCode);

    /**
     * Get the Bucket Factory
     * @return
     */
    public BucketFactory getBucketFactory();

    /**
     * Get the
     * @return
     */
    AtomicReferenceArray<Bucket> get();

    /**
     * Notify a bucket is going to scale out to two buckets
     * @param oldBucket
     * @param newBuckets
     * @return
     */
    public int onSplit(Bucket oldBucket, Bucket[] newBuckets);

    /**
     * Notify a bucket is going to scale down
     * @param oldBucket
     * @return
     */
    public int onMerge(Bucket oldBucket);
}
