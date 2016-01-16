package org.joyfulmonster.util;

/**
 * Created by wbao on 1/12/2016.
 */
public class TestUtil {
    public static void printMetrics(ConcurrentExtendiableHashMap map) {
        System.out.println("Map size=" + map.size() + " bucketCount=" + map.getMetrics().getBucketCount() + " totalSplits=" + map.getMetrics().totalSplits() + " bucketCountDifference=" + map.getMetrics().getMaxBucketCountDifference());
    }
}
