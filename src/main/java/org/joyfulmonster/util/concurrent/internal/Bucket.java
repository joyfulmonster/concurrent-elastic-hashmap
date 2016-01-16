package org.joyfulmonster.util.concurrent.internal;

/**
 * This is the interface of a Bucket.
 *
 *  A bucket is managed by Directory
 *  A bucket holds a collection of HashEntry
 *  A bucket is uniquely identified by localDepth and bucketIdx
 *  A bucket is uniquely identified by localDepth and bucketIdx
 *  LocalDepth is the number of low-order bits in the hash code whose contents are fixed for this bucket.
 *  All entry hash codes in the bucket have the same value in those bits which is equal to bucketID
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
interface Bucket<K, V> {
    /**
     * Lock the bucket.   The locker is ReentrantLocker, the thread holding the locker can reenter.
     */
    void lock();

    /**
     * Unlock the bucket
     */
    void unlock();

    /**
     * Get the Bucket ID
     * @return
     */
    public int getBucketIdx();

    /**
     * Return the Local Depth of the bucket.
     * @return
     */
    public int getLocalDepth();

    /**
     * If a bucket need to be split, the bucket will be discarded after the split is done. The other thread should not
     * perform write operation to it.
     * @return
     */
    public boolean isInvalid();

    /**
     * Whether this bucket holds to the entries of the hashCode.   The lower bits of the hashCode should match the
     * BucketIdx.
     *
     * @param hashCode
     * @return
     */
    boolean canHandle(int hashCode);

    /**
     * Safety check to see if the Bucket size is over load factor.
     * @return
     */
    public boolean hasMoreSpace();

    /**
     * Normal write into the bucket.
     *
     * @param key
     * @param value
     * @param hashCode
     * @param replaceIfPresent
     * @return
     * @throws BucketOverflowError
     */
    public V put(K key, V value, int hashCode, boolean replaceIfPresent);

    /**
     * Put in new entry by splitting the bucket.
     *
     * @param key
     * @param value
     * @param hashCode
     * @param replaceIfPresent
     * @return
     */
    public V splitAndPut(K key, V value, int hashCode, boolean replaceIfPresent);

    /**
     * this is the method to be called during split that spread the old entries from old bucket to the new bucket
     *
     * @param key
     * @param value
     * @param hashCode
     * @return
     */
    V transferEntry(K key, V value, int hashCode);

    /**
     * Support the replace methods in java.util.concurrent.ConcurrentMap.
     *
     * @param key
     * @param hashCode
     * @param oldValue
     * @param newValue
     * @return
     */
    public V replace(K key, int hashCode, V oldValue, V newValue);

    /**
     * Read the key entry.
     * @param key
     * @param hashValue
     * @return  Null if the Key was not found,otherwise, return the value.
     */
    V get(K key, int hashValue);

    /**
     * Remove the entry if the key and value are both matching
     * @param key
     * @param hashValue
     * @param value
     * @return Null if key and value pair not found, otherwise, old value.
     */
    public V remove(K key, int hashValue, V value);
}
