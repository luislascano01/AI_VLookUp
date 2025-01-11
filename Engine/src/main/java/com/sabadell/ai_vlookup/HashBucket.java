package com.sabadell.ai_vlookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HashBucket {
	private String token;
	private ArrayList<BucketEntry> entries;
	private Map<Integer, BucketEntry> entriesByIndx;

	public HashBucket(String token) {

		this.entries = new ArrayList<BucketEntry>();
		this.entriesByIndx = new HashMap<Integer, BucketEntry>();

		this.token = token;

	}

	public String getToken() {

		return this.token;
	}
	
	public ArrayList<BucketEntry> getEntries(){
		return this.entries;
	}

	public void addEntry(BucketEntry entry) {

		int entryIdx = entry.getEntryIdx();

		if (entriesByIndx.containsKey(entryIdx)) {
			BucketEntry existingEntry = entriesByIndx.get(entryIdx);
			existingEntry.increaseWeight(entry.getWeight());

		} else {
			this.entriesByIndx.put(entryIdx, entry);
			this.entries.add(entry);

		}

	}

}
