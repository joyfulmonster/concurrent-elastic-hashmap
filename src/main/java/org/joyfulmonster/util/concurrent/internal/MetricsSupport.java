package org.joyfulmonster.util.concurrent.internal;

/**
 * This is a collection of methods that will help analyze the performance of the HashMap.
 *
 * Created by Weifeng Bao on 1/12/2016.
 */
public interface MetricsSupport {
    /**
     * Metrix that tracking how many total splits happened
     * @return
     */
    int totalSplits();

    /**
     * Metrix that tracks how many actual buckets allocated.
     * @return
     */
    int getBucketCount();

    /**
     * Metric that indicate the greatest difference of bucket counts among buckets.
     */
    int getMaxBucketCountDifference();
}
