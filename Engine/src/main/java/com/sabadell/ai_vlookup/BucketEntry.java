package com.sabadell.ai_vlookup;

public class BucketEntry {

	private int entryIdx;
	private Double weight;

	public BucketEntry(int entryIdx, Double weight) {
		this.entryIdx = entryIdx;
		this.weight = weight;

	}

	public int getEntryIdx() {
		return this.entryIdx;

	}

	public Double getWeight() {

		return this.weight;
	}
	
	public void increaseWeight(Double weight) {
		this.weight += weight;
	}

}