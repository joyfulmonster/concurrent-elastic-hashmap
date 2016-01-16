package org.joyfulmonster.util.concurrent;

import org.joyfulmonster.util.concurrent.ConcurrentElasticHashMap;

/**
 * Created by Weifeng Bao on 1/12/2016.
 */
public class TestUtil {
    public static void printMetrics(ConcurrentElasticHashMap map) {
        System.out.println("Map size=" + map.size() + " bucketCount=" + map.getMetrics().getBucketCount() + " totalSplits=" + map.getMetrics().totalSplits() + " bucketCountDifference=" + map.getMetrics().getMaxBucketCountDifference());
    }
}
