package com.sabadell.ai_vlookup;

import java.util.HashMap;
import java.util.Map;

/**
 * A Pool class that extends HashMap to manage HashBucket objects. Provides
 * additional functionality to acquire, release, and manage buckets.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value (HashBucket).
 */
public class Pool {

	private Map<String, HashBucket> hashPool;

	public Pool() {

		hashPool = new HashMap<String, HashBucket>();

	}

	public HashBucket getHashBucket(String token) {

		if (hashPool.containsKey(token)) {
			return hashPool.get(token);

		}

		return null;
	}

	public void placeEntry(String token, BucketEntry entry) {

		HashBucket currHashBucket = this.getHashBucket(token);

		if (currHashBucket != null) {
			
			currHashBucket.addEntry(entry);
			

		} else if (currHashBucket == null) {
			HashBucket newHashBucket = new HashBucket(token);
			newHashBucket.addEntry(entry);
			hashPool.put(token, newHashBucket);
		}

	}

}
