package com.sabadell.ai_vlookup;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupBlock implements Serializable {
	private static final long serialVersionUID = 1L;

	String groupName;
	private Map<String, Double> headerWeightMap;
	private int blockSize;
	private Object data; // In our usage this will hold the Pool object; however, this is left abstract
							// for best practice
							// and possible future usability.

	public GroupBlock(String groupName, String[] headers, Double[] weights, Double overallWeight) throws IOException {

		if (groupName == null) {
			throw new IOException("Group block name can't be null");
		}

		if (headers == null || weights == null) {

			throw new IOException("Invalid headers and weights for GroupBlock object.");

		}

		if (headers.length != weights.length) {

			throw new IOException("Headers and weights must be of same length.");

		}

		if (overallWeight == 0.0) {

			throw new IOException("Overall group weight can't be zero.");

		}

		this.blockSize = headers.length;

		this.groupName = groupName;

		this.headerWeightMap = new HashMap<String, Double>();

		for (int i = 0; i < this.blockSize; i++) {

			this.headerWeightMap.put(headers[i], weights[i]);

		}

		// Call softmax right away:
		softmaxWeights();

	}

	/**
	 * Applies a softmax transform to the existing weights so they sum to 1. That
	 * is, each weight w is replaced by exp(w)/Σ(exp(w_i)).
	 */
	public void softmaxWeights() {
		// 1) Compute the sum of exponentials
		double sumExp = 0.0;
		for (Double w : headerWeightMap.values()) {
			sumExp += Math.exp(w);
		}
		// 2) Update each weight to exp(w)/sumExp
		for (Map.Entry<String, Double> e : headerWeightMap.entrySet()) {
			double oldVal = e.getValue();
			double newVal = Math.exp(oldVal) / sumExp;
			e.setValue(newVal);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("GroupBlock: ").append(groupName).append("\n").append("  blockSize: ").append(blockSize).append("\n")
				.append("  headerWeightMap:\n");
		for (Map.Entry<String, Double> e : headerWeightMap.entrySet()) {
			sb.append("    ").append(e.getKey()).append(" = ").append(String.format("%.4f", e.getValue())) // optional
																											// formatting
					.append("\n");
		}
		return sb.toString();
	}

	public Double getHeaderWeight(String header) {

		return headerWeightMap.get(header);
	}

	public int getBlockSize() {
		return this.blockSize;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public Object getData() {
		return this.data;
	}

	public List<String> getHeaders() {
		return new ArrayList<String>(headerWeightMap.keySet());
	}

	public String getGroupBlockName() {
		return this.groupName;
	}

}
