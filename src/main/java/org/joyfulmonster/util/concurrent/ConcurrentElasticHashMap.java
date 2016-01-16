package org.joyfulmonster.util.concurrent;

import org.joyfulmonster.util.concurrent.internal.ConcurrentElasticHashMapImpl;
import org.joyfulmonster.util.concurrent.internal.MetricsSupport;

/**
 * This is a simplified implementation of a CurrentHashMap based on Extendiable Hashing algorithm.
 * https://en.wikipedia.org/wiki/Extendible_hashing.
 *
 * The goal of this implementation focus on implementing the core algorithm for the highly concurrent and large
 * HashMap. The key APIs of ConcurrentHashMap are implemented here.
 *
 * The map didn't implement the full interface of java.util.Map or java.util.ConcurrentMap.
 * or java.io.Serializable.as of today.  It will be expanded.
 *
 * @param <K> type of keys stored in the map
 * @param <V> type of values stored in the map
 */
public class ConcurrentElasticHashMap<K, V> {
    private static final String ILLEGAL_ARGUMENT_EXPECTION_MSG = "The key or value can not be null.";

    private ConcurrentElasticHashMapImpl<K, V> service;

    public ConcurrentElasticHashMap() {
        service = new ConcurrentElasticHashMapImpl<K, V>();
    }

    public ConcurrentElasticHashMap(int bucketSize, int initBucketCount, float loadFactor) {
        service = new ConcurrentElasticHashMapImpl<K, V>(bucketSize, initBucketCount, loadFactor);
    }

    /**
     * Insert a key value pair.
     *
     * @param key  key - no null
     * @param value value - no null
     * @return  old value if Key entry exists.  null if the Key entry not exist.
     */
    public V put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.put(key, value);
    }

    /**
     * Insert a key value pair if not existing
     *
     * @param key  key - no null
     * @param value value - no null
     * @return  old value if Key entry exists.  null if the Key entry not exist.
     */
    public V putIfAbsent(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.putIfAbsent(key, value);
    }

    /**
     * Fetch the value of the key entry
     *
     * @param key
     * @return
     */
    public V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.get(key);
    }

    /**
     * Remove the entry of the key.
     * @param key
     * @return  old value if entry exists, otherwise, null pointer.
     */
    public V remove(K key) {
        if (key == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.remove(key);
    }

    /**
     * remove a key enty if the entry value is "val".
     * @param key
     * @param val
     * @return true if removed, otherwise false.
     */
    public boolean remove(K key, V val) {
        if (key == null || val == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.remove(key, val);
    }

    /**
     * repalce a key enty with new value.
     * @param key
     * @param value
     * @return true if removed, otherwise false.
     */
    public V replace(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.replace(key, value);
    }

    /**
     * @param key
     * @param oldValue
     * @param value
     * @return
     */
    public boolean replace(K key, V oldValue, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(ILLEGAL_ARGUMENT_EXPECTION_MSG);
        }
        return service.replace(key, oldValue, value);
    }

    /**
     * The total number of entries in the table
     * @return
     */
    public int size() {
        return service.size();
    }

    /**
     * Package wide method, allow client to inspect the metrics of the hashmap.
     * @return
     */
    MetricsSupport getMetrics() {
        return service;
    }
}