package org.joyfulmonster.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Sanity put/get/delete function test without concurrency.
 * 
 * Created by wbao on 1/11/2016.
 */

public class BasicTest {
    @Test(expected=IllegalArgumentException.class)
    public void testParameterPutWithNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.put(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterPutWithNullValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.put("welcome", null);
    }


    @Test(expected=IllegalArgumentException.class)
    public void testParameterGetWithNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.get(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterPutIfAbsentWithNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.putIfAbsent(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterPutIfAbsentWithNullValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.putIfAbsent("HelloWorld", null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterRemoveWithNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.remove(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterRemoveWithNullValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.remove("HelloWorld", null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterReplaceWithNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.replace(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterReplaceWithNullValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.replace("HelloWorld", null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterReplaceKVVNullKey() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.replace(null, 1, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterReplaceKVVNullOldValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.replace("Hello", null, 2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testParameterReplaceKVVNullNullValue() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        map.replace("Hello", 1, null);
    }

    @Test
    public void testBucketSizeIsPowerOf2()  {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>(4096, 11, 0.7f);
        map.put("hello1", 2);
        int bucketCount = map.getMetrics().getBucketCount();
        Assert.assertEquals("", bucketCount, 16);
    }

    @Test
    public void testPut()  {
        final ConcurrentExtendiableHashMap<String, String> map = new ConcurrentExtendiableHashMap<>();
        Assert.assertEquals(map.size(), 0);
        map.put("hello", "abc");
        Assert.assertEquals(map.size(), 1);
    }

    /**
     * Test basic put and then get
     */
    @Test
    public void testGet() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        String key = "welcome";
        map.put(key, 1);
        Assert.assertEquals(map.size(), 1);
        Integer val = map.get(key);
        Assert.assertEquals(1, val.intValue());
    }

    /*
     * Confirms that remove(key, value) only removes the entry if the key maps to the specified value.
     */
    @Test
    public void testRemoveValue() {
        final ConcurrentExtendiableHashMap<String, String> map = new ConcurrentExtendiableHashMap<>();
        String key = "welcome";
        map.put(key, "a");
        Assert.assertEquals(map.size(), 1);
        Assert.assertTrue(map.remove(key, "a"));
        map.put(key, "b");
        Assert.assertFalse(map.remove(key, "a"));
        Assert.assertEquals("b", map.get(key));
    }

    /*
     * Test the size() is correct
     */
    @Test
    public void testSize() {
        final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
        Assert.assertTrue(map.size() == 0);
        String key = "Hello";
        map.put(key, 1);
        Assert.assertTrue(map.size() == 1);
        map.put(key, 2);
        Assert.assertTrue(map.size() == 1);
        String key1 = "Hello1";
        map.put(key1, 1);
        Assert.assertTrue(map.size() == 2);
    }

    /**
     * Test the hash function can spread the load among buckets
     */
    @Test
    public void testHashEvenlyDistributedAmongBuckets() {
        int num = 2000000;
        {
            final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
            /** a list of sequence hash code */
            for (int i = 0; i < num; i++) {
                String key = new Integer(i).toString();
                // String.hashCode() will generate the hashcode which is clustered for the consequent strings
                map.put(key, i);
            }
            TestUtil.printMetrics(map);
            int balance = map.getMetrics().getMaxBucketCountDifference();
            float balanceRatio = (float) balance / (float) map.size();
            Assert.assertTrue("The balanceRatio for sequence hashcode should be smaller than 0.5", balanceRatio < 0.5f);
        }

        {
            final ConcurrentExtendiableHashMap<String, Integer> map = new ConcurrentExtendiableHashMap<>();
            RandomStringSet random = new RandomStringSet(num, 32, System.currentTimeMillis());
            for (int i = 0; i < num; i++) {
                /** a list of random hash code */
                map.put(random.get(i), i);
            }
            TestUtil.printMetrics(map);
            int balance = map.getMetrics().getMaxBucketCountDifference();
            float balanceRatio = (float) balance / (float) map.size();
            Assert.assertTrue("The balanceRatio should be smaller than 0.5 for random set strings", balanceRatio < 0.5f);
        }
    }
}
