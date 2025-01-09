package com.sabadell.ai_vlookup;

import java.util.ArrayList;

public class HashBucket {
	private String token;
	private ArrayList<BucketEntry> entries;

	public HashBucket(String token) {

		this.entries = new ArrayList<BucketEntry>();

		this.token = token;

	}

	public String getToken() {

		return this.token;
	}

	public void addEntry(BucketEntry entry) {

		entries.add(entry);
	}

}
