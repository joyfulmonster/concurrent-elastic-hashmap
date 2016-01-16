package org.joyfulmonster.util.concurrent.internal;

import java.util.Map;

/**
 * The Entry that stores into Bucket.
 *
 * Created by Weifeng Bao on 1/11/2016.
 */
class HashEntry<K,V> implements Map.Entry<K, V> {
    private final K key;
    private final V value;
    private transient final int keyHashCode;

    HashEntry(K key, V value, int keyHashCode) {
        this.key = key;
        this.value = value;
        this.keyHashCode = keyHashCode;
    }

    private HashEntry(K key, V value) {
        this(key, value, HashStrategy.getInstance().hash(key.hashCode()));
    }

    private HashEntry(Map.Entry<? extends K, ? extends V> entry) {
        this(entry.getKey(), entry.getValue());
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Map.Entry) {
            Map.Entry e = (Map.Entry) obj;
            return getKey().equals(e.getKey()) && getValue().equals(e.getValue());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return key.hashCode() ^ value.hashCode();
    }

    int getKeyHashCode() {
        return keyHashCode;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
