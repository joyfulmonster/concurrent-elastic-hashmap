package org.joyfulmonster.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The testsuite that testing concurrent access to the map.
 */
public class ConcurrencyTest {
	/*
	 * Runs concurrent put test with 8 threads. If the test system has a larger number of cores, consider increasing
	 * the thread count.
	 *
	 * It will trigger more split operations.
	 */
	@Test	
	public void testConcurrentPutsDifferentKeys() throws InterruptedException, ExecutionException {
		ConcurrentExtendiableHashMap<String,Integer> map = new ConcurrentExtendiableHashMap<>();
		runConcurrentPutDifferentKeys(map, 8, 100000);
		TestUtil.printMetrics(map);
	}

    @Test
    public void testConcurrentPutsSameKeys() throws InterruptedException, ExecutionException {
        ConcurrentExtendiableHashMap<String,Integer> map = new ConcurrentExtendiableHashMap<>();
        runConcurrentPutSameKeySet(map, 10, 100000);
        TestUtil.printMetrics(map);
    }

    /*
	 * Runs concurrent put test with larger bucket sizes and loads
	  *
	  * 2 ^ 18 = 262144
	 */
	@Test
	public void testCompareSmallAndLargerBuckets() throws InterruptedException, ExecutionException {
		ConcurrentExtendiableHashMap<String,Integer> regularMap = new ConcurrentExtendiableHashMap<>();
		runConcurrentPutDifferentKeys(regularMap, 4, 800000);
		TestUtil.printMetrics(regularMap);

		ConcurrentExtendiableHashMap<String,Integer> map = new ConcurrentExtendiableHashMap<>(262144, 2, 0.9f);
		runConcurrentPutDifferentKeys(map, 4, 800000);
		TestUtil.printMetrics(map);

		Assert.assertTrue("Expect the total splits greater than ", map.getMetrics().totalSplits() < regularMap.getMetrics().totalSplits());
		Assert.assertTrue("Expect the total buckets greater than ", map.getMetrics().getBucketCount() < regularMap.getMetrics().getBucketCount());
	}

	/**
	 * Run conccurent put/get with 6 threads
	 *
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@Test
	public void testConcurrentGetOnSameKeySets() throws InterruptedException, ExecutionException {
		ConcurrentExtendiableHashMap<String, Integer> map =
				new ConcurrentExtendiableHashMap<>();
		runConcurrentGetOnSameKeySets(map, 6, 20000, 4000000L);
		TestUtil.printMetrics(map);
	}
	/*
	 * Runs concurrent put/remove/get test with 16 threads.
	 */
	@Test
	public void testConcurrentPutRemoveGet() throws InterruptedException, ExecutionException {
		ConcurrentExtendiableHashMap<String,Integer> map = new ConcurrentExtendiableHashMap<>();
		runConcurrentPutRemoveGet(map, 6, 20000, 1000000L);
		TestUtil.printMetrics(map);
	}

	/**
	 * Run the current put/remove/get operations
	 * @param map    the map
	 * @param threadCount   the number of parallel threads
	 * @param keyCount   the total key/value pairs genearted.
	 * @param totalExcutionTimes
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void runConcurrentPutRemoveGet(final ConcurrentExtendiableHashMap<String, Integer> map,
										  final int threadCount, final int keyCount,
                                          final long totalExcutionTimes)
			throws InterruptedException, ExecutionException {
		
		final RandomStringSet keySet = new RandomStringSet(keyCount, 64, System.currentTimeMillis());

		final ConcurrentLinkedQueue<Integer> removedValues = new ConcurrentLinkedQueue<>();

		/** Put into hashmap, the value of the key is the index of the key in the keySet */
		for (int i = 0; i < keyCount; i++) {
			map.put(keySet.get(i), new Integer(i));
		}

        int removeCounts = (int)(keyCount / 4);
		/**
		 * randomly remove a list of entries
		 */
		Random random = new Random(System.currentTimeMillis());
		for (int i = 0; i < removeCounts; i++) {
			int removeIdx = random.nextInt(keyCount);
			String key = keySet.get(removeIdx);
			Integer val = map.remove(key);
			while (val == null) {
				removeIdx = random.nextInt(keyCount);
				key = keySet.get(removeIdx);
				val = map.remove(key);
			}
			removedValues.offer(val);
		}

        System.out.println ("Remove done " + removeCounts);

		final AtomicLong repeatTimes = new AtomicLong(0L);
		
		Callable<Long> putAndRemover = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				// in the thread
				long totalRepeatTimes = repeatTimes.getAndIncrement();
				long localRepeatedTimes = 0L;

                /**
                 * Put and remove again and again
                 */
				Random random = new Random(System.currentTimeMillis() + 100000);
				while (totalRepeatTimes < totalExcutionTimes) {
					Integer removedValue = removedValues.poll();
					Assert.assertNotNull(removedValue);

					Integer oldValue = map.put(keySet.get(removedValue.intValue()), removedValue);
					Assert.assertNull("The value sould have been removed, but we got this value", oldValue);
					
					int removeIndex = random.nextInt(keyCount);
					Integer removedVal = map.remove(keySet.get(removeIndex));
					while (removedVal == null) {
						removeIndex = random.nextInt(keyCount);
						removedVal = map.remove(keySet.get(removeIndex));
					}
					Assert.assertEquals("removed value mismatch", removedVal.intValue(), removeIndex);
					removedValues.offer(removedVal);
					localRepeatedTimes++;
					totalRepeatTimes = repeatTimes.getAndIncrement();
				}
				
				Integer val = removedValues.poll();
				while (val != null) {
					Integer inMap = map.put(keySet.get(val.intValue()), val);
					Assert.assertNull("unexpected recycle value already in map", inMap);	
					val = removedValues.poll();
				}
				
				return localRepeatedTimes;
			}
		};
		
		Callable<Long> getter = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				long totalGets = 0L;
				Random random = new Random(System.currentTimeMillis() + 20000);
				while (repeatTimes.get() < totalExcutionTimes) {
					int getIndex = random.nextInt(keyCount);
					Integer val = map.get(keySet.get(getIndex));
					if (val != null) {
						Assert.assertEquals(getIndex, val.intValue());
					}
                    else {
                        // it might have been removed
                    }
					totalGets++;
				}
				return totalGets;
			}
		};

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

		List<Callable<Long>> putRemoveTasks = Collections.nCopies(threadCount / 2, putAndRemover);
        LinkedList<Future<Long>> results = new LinkedList<>();
        for (Callable<Long> task : putRemoveTasks) {
            results.add(executorService.submit(task));
        }

        List<Callable<Long>> getTasks = Collections.nCopies(threadCount/2, getter);
        LinkedList<Future<Long>> getResults = new LinkedList<>();
        for (Callable<Long> task : getTasks) {
            getResults.add(executorService.submit(task));
        }

        long totalRecycles = 0L;
        for (Future<Long> result : results) {
        	totalRecycles += result.get();
        }
        for (Future<Long> result : getResults) {
            result.get();
        }

        Assert.assertEquals("total round mismatch", totalRecycles, totalExcutionTimes);
		Assert.assertEquals("key count map size mismatch", keyCount, map.size());
	}

	/**
	 * Run current GET operation
	 * @param map
	 * @param threadCount
	 * @param keyCount
	 * @param runTimes
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void runConcurrentGetOnSameKeySets(final ConcurrentExtendiableHashMap<String, Integer> map, int threadCount, final int keyCount, final long runTimes)
			throws InterruptedException, ExecutionException {

		final RandomStringSet keySet = new RandomStringSet(keyCount, 32, 1337L);

		/** Put key values to map */
		for (int i = 0; i < keySet.size(); i++) {
			map.put(keySet.get(i), new Integer(i));
		}

		Callable<Long> getTask = new Callable<Long>() {
			@Override
			public Long call() {
				long getCount = 0;
                int next = 0;
				while (getCount++ < runTimes) {
					String key = keySet.get(next);
					Integer i = map.get(key);
					if (i == null) {
						continue;
					}
					else {
						Assert.assertEquals("the value should match ", new Integer(next), i);
					}
                    if (next >= keySet.size()) {
                        next = 0;
                    }
				}
				return getCount;
			}
		};

		List<Callable<Long>> getTasks = Collections.nCopies(threadCount, getTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Long>> futures = executorService.invokeAll(getTasks);
        Assert.assertEquals("future count should equal threadCount", threadCount, futures.size());

        long totalGets = 0;
        for (Future<Long> future : futures) {
            totalGets += future.get();
        }
	}

	/**
	 * Execute the current put tests
	 *
	 * @param map
	 * @param threadCount   number of current threads
	 * @param keySetSize  how many entries to be put
	 *
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void runConcurrentPutDifferentKeys(final ConcurrentExtendiableHashMap<String, Integer> map, int threadCount, final int keySetSize)
			throws InterruptedException, ExecutionException {

		final RandomStringSet keySet = new RandomStringSet(keySetSize, 64, 8888L);

		final AtomicInteger nextKey = new AtomicInteger(0);
		
		Callable<Integer> putTask = new Callable<Integer>() {
			@Override
			public Integer call() {
				int putCount = 0;
				int next = nextKey.getAndIncrement(); 
				while (next < keySet.size()) {
					String key = keySet.get(next);
					Integer val = new Integer(next);
					Integer inMap = map.put(key, val);
					Assert.assertNull(inMap);
					putCount++;
					next = nextKey.getAndIncrement();
				}
				System.out.println ("Done put job thread <" + Thread.currentThread().getId() + "> putCount=" + putCount + " totalKeys=" + keySetSize);
				return putCount;
			}
		};
		
		List<Callable<Integer>> putTasks = Collections.nCopies(threadCount, putTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = executorService.invokeAll(putTasks);
        Assert.assertEquals("The actual thread count is different from the future count", threadCount, futures.size());

        int totalPuts = 0;
        for (Future<Integer> future : futures) {
            totalPuts += future.get();
        }
        Assert.assertEquals("keyCount and total put operation mismatch", keySet.size(), totalPuts);
        Assert.assertEquals("keyCount and hashmap size mismatch", keySet.size(), map.size());

        for (int i = 0; i < keySet.size(); i++) {
        	Integer val = map.get(keySet.get(i));
        	Assert.assertNotNull(String.format("Entry for key " + i + " missed from map", i), val);
        	Assert.assertEquals("wrong value for key in map", val.intValue(), i);
        }
	}

    /**
     * Execute the current put tests
     *
     * @param map
     * @param threadCount   number of current threads
     * @param keySetSize  how many entries to be put
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void runConcurrentPutSameKeySet(final ConcurrentExtendiableHashMap<String, Integer> map, int threadCount, final int keySetSize)
            throws InterruptedException, ExecutionException {

        final RandomStringSet keySet = new RandomStringSet(keySetSize, 32, 8888L);

        Callable<Integer> putTask = new Callable<Integer>() {
            @Override
            public Integer call() {
                int putCount = 0;
                int next = 0;
                while (next < keySet.size()) {
                    String key = keySet.get(next);
                    Integer val = new Integer(next);
                    map.put(key, val);
                    putCount++;
                    next++;
                }
                System.out.println("Done put job thread <" + Thread.currentThread().getId() + "> putCount=" + putCount + " totalKeys=" + keySetSize);
                return putCount;
            }
        };

        List<Callable<Integer>> putTasks = Collections.nCopies(threadCount, putTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = executorService.invokeAll(putTasks);
        Assert.assertEquals("The actual thread count is different from the future count", threadCount, futures.size());

        int totalPuts = 0;
        for (Future<Integer> future : futures) {
            totalPuts += future.get();
        }
        Assert.assertEquals("keyCount and total put operation mismatch", keySet.size() * threadCount, totalPuts);
        Assert.assertEquals("keyCount and hashmap size mismatch", keySet.size(), map.size());

        for (int i = 0; i < keySet.size(); i++) {
            Integer val = map.get(keySet.get(i));
            Assert.assertNotNull(String.format("Entry for key " + i + " missed from map", i), val);
            Assert.assertEquals("wrong value for key in map", val.intValue(), i);
        }
    }
}
