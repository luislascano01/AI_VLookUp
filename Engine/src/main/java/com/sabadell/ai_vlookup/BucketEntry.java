package com.sabadell.ai_vlookup;

import java.io.Serializable;

public class BucketEntry implements Serializable {
	private static final long serialVersionUID = 1L;
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