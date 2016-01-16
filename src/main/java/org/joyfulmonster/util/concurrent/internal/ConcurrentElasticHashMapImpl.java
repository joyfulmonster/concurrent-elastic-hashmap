package org.joyfulmonster.util.concurrent.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The entrypoint implementation of the hashmap.
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
public class ConcurrentElasticHashMapImpl<K, V> implements MetricsSupport {

    private static final String ILLEGAL_ARGUMENT_EXPECTION_MSG = "The key or value can not be null.";

    /**
     * Default Configuration Parameters to this map.
     */
    /** The default bucket size, it may need to be tuned based on machine architecture */
    public static final int DEFAULT_BUCKET_SIZE = 8092; // 8K block
    /** The default intiail bucket count */
    public static final int DEFAULT_BUCKET_COUNT = 8;
    /** the default load factor for bucket */
    public static final float DEFAULT_BUKCET_LOAD_FACTOR = 0.75f;

    /** the max capacity, followed the convention used by java.lang.HashMap */
    public static final int MAX_CAPACITY = 1 << 30;
    /** the minimal bucket count */
    public static final int MIN_BUCKET_COUNT = 2;
    /** caluate the max bucket count */
    public static final int MAX_BUCKET_COUNT = MAX_CAPACITY / MIN_BUCKET_COUNT;
    /** The max loadfactor for a bucket */
    public static final float MAX_BUCKET_LOADFACTOR = 0.95f;

    /** reference to Directory */
    private final Directory directory;
    /** metric: total splitted time */
    private final AtomicInteger totalSplitCount;
    /** metric: total number of entries */
    private final AtomicInteger totalEntryCount;

    /**
     * Creates a map with default configuration parameters.
     */
    public ConcurrentElasticHashMapImpl() {
        this(DEFAULT_BUCKET_SIZE, DEFAULT_BUCKET_COUNT, DEFAULT_BUKCET_LOAD_FACTOR);
    }

    /**
     * Constructor create a ConcurrentElasticHashMap
     *
     * @param bucketSize        The number of entries in a bucket.
     * @param initBucketCount  The initial buckets to be allocated, it maps to the depth of Directory.
     * @param bucketLoadFactor    The bucket load factor.
     */
    public ConcurrentElasticHashMapImpl(int bucketSize, int initBucketCount, float bucketLoadFactor) {
        totalSplitCount = new AtomicInteger(0);
        totalEntryCount = new AtomicInteger(0);
        // the bucketCount equals to directory size, it needs to be a power 2 value.
        initBucketCount = lowestUpperBound(initBucketCount);
        // if the loadfactor is larger than 1, there maybe some unexpected behavior, regulate the value here.
        bucketLoadFactor = (bucketLoadFactor < MAX_BUCKET_LOADFACTOR) ? bucketLoadFactor : MAX_BUCKET_LOADFACTOR;
        directory = DirectoryFactory.create(initBucketCount, bucketSize, bucketLoadFactor, totalEntryCount, totalSplitCount);
    }

    /**
     * Inert key/value pair.  Override the value if key entry already existed.
     * @param key
     * @param value
     * @return
     */
    public V put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return put(key, value, true);
    }

    /**
     * API from java.util.concurrent.CurrentMap.  Add the key if key not existing.
     * @param key
     * @param value
     * @return
     */
    public V putIfAbsent(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return put(key, value, false);
    }

    /**
     * The implementation of the put logic.
     *
     * @param key
     * @param value
     * @param replaceIfPresent
     * @return
     */
    private V put(K key, V value, boolean replaceIfPresent) {
        int hashCode = HashStrategy.getInstance().hash(key.hashCode());

        while (true) {
            Bucket<K, V> bucket = directory.getBucket(hashCode);
            bucket.lock();
            if (bucket.isInvalid()) {
                /**
                 * it means the bucket is being splited, so it is not allowed to write to the bucket any more.
                 * unlock the bucket and ask Directory to give back the new Bucket after split is done.
                 */
                bucket.unlock();
                continue;
            }
            else {
                try {
                    V result = null;
                    boolean putSuccess;
                    if (bucket.hasMoreSpace()) {
                        try {
                            result = (V) bucket.put(key, value, hashCode, replaceIfPresent);
                            putSuccess = true;
                        } catch (BucketOverflowError soe) {
                            putSuccess = false;
                        }
                    } else {
                        putSuccess = false;
                    }

                    if (!putSuccess) {
                        // the normal put failed, so go ahead split the bucket and put the entry in.
                        result = bucket.splitAndPut(key, value, hashCode, replaceIfPresent);
                    }
                    return result;
                } finally {
                    bucket.unlock();
                }
            }
        }
    }

    /**
     * Remove the key entry if value also match
     * @param key
     * @param value
     * @return
     */
    public boolean remove(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        int hashValue = HashStrategy.getInstance().hash(key.hashCode());
        while (true) {
            Bucket bucket = directory.getBucket(hashValue);
            bucket.lock();
            try {
                if (!bucket.isInvalid()) {
                    /**
                     * it means the bucket is being splited, so it is not allowed to write to the bucket any more.
                     * unlock the bucket and ask Directory to give back the new Bucket after split is done.
                     */
                    return bucket.remove(key, hashValue, value) != null;
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    /**
     * Replace the key entry with value
     *
     * @param key
     * @param value
     * @return
     */
    public V replace(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        int hashValue = HashStrategy.getInstance().hash(key.hashCode());
        while (true) {
            Bucket bucket = directory.getBucket(hashValue);
            bucket.lock();
            try {
                if (!bucket.isInvalid()) {
                    /**
                     * it means the bucket is being splited, so it is not allowed to write to the bucket any more.
                     * unlock the bucket and ask Directory to give back the new Bucket after split is done.
                     */
                    return (V) bucket.replace(key, hashValue, null, value);
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    /**
     * The API from java.util.concurrent.ConcurrentMap
     * Replace the key entry whose value is oldvalue with newValue
     * @param key
     * @param oldValue
     * @param newValue
     * @return
     */
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        int hashValue = HashStrategy.getInstance().hash(key.hashCode());
        while (true) {
            Bucket bucket = directory.getBucket(hashValue);
            bucket.lock();
            try {
                if (!bucket.isInvalid()) {
                    return bucket.replace(key, hashValue, oldValue, newValue) != null;
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    /**
     * Get the key value.
     * @param key
     * @return
     */
    public V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        int hashCode = HashStrategy.getInstance().hash(key.hashCode());
        Bucket<K, V> bucket = directory.getBucket(hashCode);
        return bucket.get(key, hashCode);
    }

    /**
     * Remove the key entry
     * @param key
     * @return
     */
    public V remove(K key) {
        if (key == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        int hashValue = HashStrategy.getInstance().hash(key.hashCode());
        while (true) {
            Bucket<K, V> bucket = directory.getBucket(hashValue);
            bucket.lock();
            try {
                if (!bucket.isInvalid()) {
                    return (V) bucket.remove(key, hashValue, null);
                }
            } finally {
                bucket.unlock();
            }
        }
    }

    /**
     * Return the number of the entries in the table
     * @return
     */
    public int size() {
        return totalEntryCount.get();
    }

    /**
     * Return the metric that tracking how many total splits happened
     * @return
     */
    public int totalSplits() {
        return totalSplitCount.get();
    }

    /**
     * Return the metric that how many buckets are allocated.
     * @return
     */
    public int getBucketCount() {
        return directory.getBucketCount();
    }

    @Override
    public int getMaxBucketCountDifference() {
        AtomicReferenceArray<Bucket> buckets = directory.get();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i=0; i<buckets.length(); i++) {
            Bucket bucket = buckets.get(i);
            if (bucket instanceof BucketMetricsSupport) {
                int entries = ((BucketMetricsSupport)bucket).getBucketEntries();
                if (entries < min) {
                    min = entries;
                }
                if (entries > max) {
                    max = entries;
                }
            }
        }
        return max-min;
    }

    /**
     * Find the the smallest 2 power value that is larger than i
     * @param
     * @return
     */
    private int lowestUpperBound (int i) {
        int seed = 1;
        while (seed < i) {
            seed = seed << 1;
        }

        return seed;
    }
}