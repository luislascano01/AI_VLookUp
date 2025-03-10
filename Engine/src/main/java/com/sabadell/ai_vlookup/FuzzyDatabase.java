package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import info.debatty.java.stringsimilarity.Damerau;

/**
 * Represents a fuzzy logic-based database that allows data to be tokenized and
 * indexed for efficient lookup operations. It uses a configurable backbone (via
 * a YAML file) to manage the relationships between data groups and headers
 */

public class FuzzyDatabase implements Serializable {

	private static final long serialVersionUID = 1L;

	private BidirectionalGroupMap backbone; // Backbone configuration for managing headers and groups
	private String name; // Name of the database
	private Dataframe sourceDataFrame;
	private Double maxWeight = Double.MIN_VALUE;

	/**
	 * Constructs a new FuzzyDatabase instance with the given name and YAML
	 * configuration file.
	 *
	 * @param name The name of the database.
	 * @param yaml The YAML file defining the backbone configuration.
	 */
	public FuzzyDatabase(String name, File yaml) {
		try {
			backbone = new BidirectionalGroupMap(yaml);
		} catch (IOException e) {
			System.err.println("Unable to load backbone configuration");
			e.printStackTrace();
		}
		this.name = name;
	}

	/**
	 * Saves the current state of the FuzzyDatabase instance to a file.
	 * 
	 * @param filePath The full path to the file where the database should be saved.
	 *                 This method serializes the entire FuzzyDatabase object and
	 *                 writes it to the specified file. If the parent directories of
	 *                 the file do not exist, they will be created. If the file does
	 *                 not exist, it will be created automatically. If the file
	 *                 exists, it will be overwritten. Proper error handling is
	 *                 implemented to catch and log any IO exceptions that may occur
	 *                 during the process.
	 */
	public void saveDatabaseToFile(String filePath) {
		try {
			// Prepare the file object and check if parent directories need to be created
			File file = new File(filePath);
			File parentDirectory = file.getParentFile();
			if (parentDirectory != null && !parentDirectory.exists()) {
				parentDirectory.mkdirs(); // Create the directory structure if it does not exist
			}

			// Create a file output stream wrapped by an object output stream
			try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
				out.writeObject(this); // Serialize this FuzzyDatabase instance to the file
			}
		} catch (IOException e) {
			// Log exceptions to standard error
			e.printStackTrace();
		}
	}

	/**
	 * Loads a FuzzyDatabase instance from a specified file.
	 * 
	 * @param filePath The full path to the file from which the database should be
	 *                 loaded.
	 * @return The deserialized FuzzyDatabase instance, or null if an error occurs.
	 *         This method deserializes a FuzzyDatabase object from the file
	 *         specified. Proper error handling is implemented to catch and log IO
	 *         and ClassNotFound exceptions that may occur.
	 */
	public static FuzzyDatabase loadDatabaseFromFile(String filePath) {
		FuzzyDatabase database = null; // Initialize the database object to null
		// Create a file input stream wrapped by an object input stream
		try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filePath))) {
			database = (FuzzyDatabase) in.readObject(); // Deserialize the object from the file
		} catch (IOException | ClassNotFoundException e) {
			// Log exceptions to standard error
			e.printStackTrace();
		}
		return database; // Return the deserialized database object or null if an exception was caught
	}

	/**
	 * Loads data into the database from a given Dataframe. Each row's data is
	 * tokenized and indexed into pools based on the backbone configuration.
	 *
	 * @param df The Dataframe containing the data to be loaded.
	 */
	public void loadData(Dataframe df) {
		this.sourceDataFrame = df;
		List<String> inputHeaders = this.backbone.getInputHeaders(true);

		ProgressDisplay progressBar = new ProgressDisplay(df.size());

		System.out.println("Loading reference data...");

		for (Map<String, String> rowData : df) {
			// Iterate over all rows
			try {
				progressBar.displayProgressAuto();

			} catch (IOException e) {

				System.out.println("Error when trying to display progress bar in terminal");
			}

			for (String inputHeader : inputHeaders) {

				String currCellData = rowData.get(inputHeader); // Get cell data
				String rowIdx = rowData.get("index"); // Get row index

				// Tokenize the current cell data
				List<String> tokens = Token.tokenize(currCellData);

				// Retrieve groups associated with the current header
				List<GroupBlock> pointedGroups = this.backbone.getGroupsFromHeader(inputHeader, true);

				for (GroupBlock currBlock : pointedGroups) {
					Pool currPool;
					if (currBlock.getData() == null) {
						currPool = new Pool();
					} else if (currBlock.getData() instanceof Pool) {
						currPool = (Pool) currBlock.getData();
					} else {
						continue;
					}

					Double weight = currBlock.getHeaderWeight(inputHeader); // Header weight
					BucketEntry bucketEntry = new BucketEntry(Integer.parseInt(rowIdx), weight);

					for (String token : tokens) {
						currPool.placeEntry(token, bucketEntry); // Place token in pool
					}

					currBlock.setData(currPool); // Update group block with pool
				}
			}
		}
		try {
			progressBar.finishProgress();
		} catch (IOException e) {
			System.err.println("Error closing progress display.");
			e.printStackTrace();
		}

	}

	private void cleanPreviousQueryData() {

		Map<String, GroupBlock> toClean = this.backbone.getGroups(false);

		for (Map.Entry<String, GroupBlock> blockEntry : toClean.entrySet()) {

			blockEntry.getValue().setData(null);

		}

	}

	/**
	 * This method leverages the Hashmap-based backbone of the database to perform a
	 * quick query given some "key". By key, it is expected that the look up returns
	 * the fewer results as possible. Each backbone has a "key" column for each side
	 * of the bidirectional mapping relationship. The method returns all entries
	 * that completely match the provided key under the key column; meaning that
	 * both contents must be equal to each other before returning a "matching
	 * value".
	 * 
	 * @return ArrayList<Integer> Indexes of the matching entries.
	 */
	public List<Integer> lookUpEntryByID(String keyToLookUp, Map<String, String> rowData) {

		List<Integer> matchingIdxs = new ArrayList<Integer>();

		List<GroupBlock> idGroups = this.backbone.getGroupsFromHeader(this.backbone.getReferenceKeyHeader(), true);

		for (GroupBlock currGroup : idGroups) {

			if (currGroup.getData() != null && currGroup.getData() instanceof Pool) {

				Pool currPool = (Pool) currGroup.getData();

				HashBucket matchingBucket = currPool.getHashBucket(keyToLookUp);

				if (matchingBucket != null) {

					ArrayList<BucketEntry> matchingEntries = matchingBucket.getEntries();

					for (BucketEntry matchingEntry : matchingEntries) {

						int matchingIdx = matchingEntry.getEntryIdx();

						matchingIdxs.add(matchingIdx);

					}

				}
			}

		}

		return matchingIdxs;

	}

	/**
	 * Compares two map entries using the Damerau-Levenshtein distance. This is a
	 * simple coefficient.
	 * 
	 * A more advanced method that would carry the same purpose of this method would
	 * be to compare the overlapping buckets between the entries and find a average
	 * of both sets ratios (overlappingSize/(queryBucketSize)) TODO: Check this set
	 * logic.
	 *
	 * @param target    The target map of String pairs.
	 * @param reference The reference map of String pairs.
	 * @return A Double representing the similarity score (1.0 - max similarity, 0.0
	 *         - min similarity).
	 */
	public Double compareEntriesDamerau(Map<String, String> target, Map<String, String> reference) {
		// Concatenate all values from each map into a single string
		String targetValues = concatenateSortedValuesInput(target, false);
		String referenceValues = concatenateSortedValuesInput(reference, true);

		// Calculate Damerau-Levenshtein distance
		Damerau damerau = new Damerau();
		double distance = damerau.distance(targetValues, referenceValues);

		// Normalize distance based on the maximum possible distance (which would be the
		// length of the longer string)
		double maxLen = Math.max(targetValues.length(), referenceValues.length());
		if (maxLen == 0) {
			return 1.0; // Both are empty strings
		}

		// Normalize to get a similarity score
		return 1.0 - (distance / maxLen);
	}

	// Weights for similarity measures (adjustable)
	private static final double ALPHA = 0.6; // Weight for Damerau-Levenshtein
	private static final double BETA = 0.15; // Weight for Jaccard
	private static final double GAMMA = 0.25; // Weight for Levenshtein with swaps

	/**
	 * Compares two map entries based on the Jaccard index, a statistical measure
	 * for comparing the similarity and diversity of sample sets. This is used to
	 * calculate similarity in a manner that ignores the order of elements.
	 * 
	 * @param target    The target map of String pairs.
	 * @param reference The reference map of String pairs.
	 * @return A Double representing the similarity score (1.0 - max similarity, 0.0
	 *         - min similarity).
	 */
	public List<Map<String, String>> sortMatchesBySimilarityToQuery(Map<String, String> query,
			List<Map<String, String>> references) {
		// List to hold pairs of similarity scores and corresponding maps
		List<Pair<Double, Map<String, String>>> scoredReferences = new ArrayList<>();

		// Damerau-Levenshtein distance calculator
		Damerau damerau = new Damerau();

		// Calculate similarity for each reference and store it with the reference
		for (Map<String, String> reference : references) {
			// Get ordered concatenated values for comparison
			String queryStr = concatenateSortedValuesInput(query, false);
			String referenceStr = concatenateSortedValuesInput(reference, true);

			// Damerau-Levenshtein similarity (accounts for swaps)
			double damerauDistance = damerau.distance(queryStr, referenceStr);
			double damerauSim = normalizeDistance(damerauDistance, queryStr.length(), referenceStr.length());

			// Jaccard similarity (measures set overlap)
			double jaccardSim = compareEntriesJaccard(query, reference);

			// Levenshtein distance with swaps (traditional edit distance)
			double levenshteinDistance = levenshteinWithSwaps(queryStr, referenceStr);
			double levenshteinSim = normalizeDistance(levenshteinDistance, queryStr.length(), referenceStr.length());

			// Weighted similarity score
			double finalScore = (ALPHA * damerauSim) + (BETA * jaccardSim) + (GAMMA * levenshteinSim);

			// Store the computed score with the reference
			scoredReferences.add(new Pair<>(finalScore, reference));
		}

		// Sort the list of pairs based on the similarity score in descending order
		scoredReferences.sort((pair1, pair2) -> Double.compare(pair2.getFirst(), pair1.getFirst()));

		// Extract the sorted maps into a new list
		List<Map<String, String>> sortedReferences = new ArrayList<>();
		for (Pair<Double, Map<String, String>> pair : scoredReferences) {
			sortedReferences.add(pair.getSecond());
		}

		return sortedReferences;
	}

	/**
	 * Computes the Jaccard similarity between two map entries.
	 *
	 * @param target    The target map of String pairs.
	 * @param reference The reference map of String pairs.
	 * @return Jaccard similarity score.
	 */
	public Double compareEntriesJaccard(Map<String, String> target, Map<String, String> reference) {
		Set<String> targetValues = new HashSet<>(target.values());
		Set<String> referenceValues = new HashSet<>(reference.values());

		Set<String> intersection = new HashSet<>(targetValues);
		intersection.retainAll(referenceValues);

		Set<String> union = new HashSet<>(targetValues);
		union.addAll(referenceValues);

		return union.isEmpty() ? 1.0 : (double) intersection.size() / union.size();
	}

	/**
	 * Computes Levenshtein distance with swaps.
	 *
	 * @param str1 First string.
	 * @param str2 Second string.
	 * @return Levenshtein edit distance considering swaps.
	 */
	public static int levenshteinWithSwaps(String str1, String str2) {
		int[][] dp = new int[str1.length() + 1][str2.length() + 1];

		for (int i = 0; i <= str1.length(); i++) {
			for (int j = 0; j <= str2.length(); j++) {
				if (i == 0) {
					dp[i][j] = j;
				} else if (j == 0) {
					dp[i][j] = i;
				} else {
					int cost = (str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1;

					dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, // Deletion
							dp[i][j - 1] + 1), // Insertion
							dp[i - 1][j - 1] + cost // Substitution
					);

					if (i > 1 && j > 1 && str1.charAt(i - 1) == str2.charAt(j - 2)
							&& str1.charAt(i - 2) == str2.charAt(j - 1)) {
						dp[i][j] = Math.min(dp[i][j], dp[i - 2][j - 2] + 1); // Transposition (swap)
					}
				}
			}
		}
		return dp[str1.length()][str2.length()];
	}

	/**
	 * Normalizes distance-based similarity scores between 0 and 1.
	 *
	 * @param distance The computed distance.
	 * @param len1     Length of the first string.
	 * @param len2     Length of the second string.
	 * @return A similarity score between 0 and 1.
	 */
	private double normalizeDistance(double distance, int len1, int len2) {
		double maxLen = Math.max(len1, len2);
		return maxLen == 0 ? 1.0 : 1.0 - (distance / maxLen);
	}

	/**
	 * Concatenates sorted values of a map into a single string based on headers.
	 *
	 * @param map       The map whose values are to be concatenated.
	 * @param reference Whether it is a reference map.
	 * @return A sorted, space-separated string of the map's values.
	 */
	private String concatenateSortedValuesInput(Map<String, String> map, boolean reference) {
		ArrayList<String> values = new ArrayList<>();
		List<String> headers = this.backbone.getInputHeaders(reference);

		for (String header : headers) {
			if (map.containsKey(header)) {
				values.add(map.get(header));
			}
		}

		Collections.sort(values);
		return String.join(" ", values);
	}

	/**
	 * Simple pair class for holding two related objects.
	 */
	private static class Pair<T, U> {
		private final T first;
		private final U second;

		public Pair(T first, U second) {
			this.first = first;
			this.second = second;
		}

		public T getFirst() {
			return first;
		}

		public U getSecond() {
			return second;
		}
	}

	public boolean compareByID(Map<String, String> reference, Map<String, String> target) {

		String referenceID = reference.get(this.backbone.getReferenceKeyHeader());
		String targetID = target.get(this.backbone.getTargetKeyHeader());

		if (referenceID != null && targetID != null) {

			return referenceID.equalsIgnoreCase(targetID);
		}

		return false;

	}

	/**
	 * Performs a fuzzy lookup for a given row of data and returns matching entries.
	 * The process involves tokenizing the query row, aggregating weights, and
	 * sorting results.
	 *
	 * @param rowData The row of data to look up.
	 * @return A list of matching rows with weights, sorted by relevance.
	 */
	public ArrayList<Map<String, String>> lookUpEntry(Map<String, String> rowData) {

		this.cleanPreviousQueryData();

		ArrayList<Map<String, String>> matchingEntries = new ArrayList<Map<String, String>>();

		String keyHeaderTarget = this.backbone.getTargetKeyHeader();

		String keyToLookUp = rowData.get(keyHeaderTarget);

		if (keyToLookUp != null) {

			List<Integer> exactKeyMatchesIdxs = this.lookUpEntryByID(keyToLookUp, rowData);

			List<Map<String, String>> exactKeyMatches = this.sourceDataFrame.get(exactKeyMatchesIdxs);
			
			for (Map<String, String> entry : exactKeyMatches) {
			    entry.put("weight", String.valueOf(Double.MAX_VALUE));
			}
			
			if (exactKeyMatches.size() > 0) {
				matchingEntries.addAll(exactKeyMatches);			
				return matchingEntries;
			}
		}

		List<String> queryHeaders = backbone.getInputHeaders(false);
		QueryAnalyzer queryAnalyzer = new QueryAnalyzer(rowData, queryHeaders);

		// Tokenize query entries and associate them with corresponding groups
		for (String header : queryHeaders) {
			List<GroupBlock> targetGroupBlocks = backbone.getGroupsFromHeader(header, false);
			List<String> tokenizedEntry = queryAnalyzer.getTokenizedEntryCell(header);

			for (GroupBlock targetGroupBlock : targetGroupBlocks) {
				if (targetGroupBlock.getData() != null) {
					if (targetGroupBlock.getData() instanceof List<?>) {
						@SuppressWarnings("unchecked")
						List<String> groupTokens = (List<String>) targetGroupBlock.getData();
						groupTokens.addAll(tokenizedEntry);
					}
				} else {
					List<String> groupTokens = new ArrayList<String>(tokenizedEntry);
					targetGroupBlock.setData(groupTokens);
				}
			}
		}

		Map<String, GroupBlock> targetGroups = backbone.getGroups(false);

		// Aggregate weights using tokens and hash lookups
		for (Map.Entry<String, GroupBlock> blockEntry : targetGroups.entrySet()) {
			String blockName = blockEntry.getKey();
			GroupBlock currBlock = blockEntry.getValue();

			@SuppressWarnings("unchecked")
			List<String> groupTokens = (List<String>) currBlock.getData();

			for (GroupBlock referenceBlock : backbone.getGroupsFromSourceGroupName(blockName, false)) {
				Pool currPool = (Pool) referenceBlock.getData();

				for (String token : groupTokens) {
					if (currPool != null) {
						HashBucket currBucket = currPool.getHashBucket(token);
						

						if (currBucket != null) {
							for (BucketEntry entry : currBucket.getEntries()) {
								Integer idx = entry.getEntryIdx();
								Integer x = token.length();
								Double c = 2.0;
								Double s = -7.0;
								Double m = 0.2;
								Double increasingFactor = Math.exp((x + s) / c) - Math.exp(s / c) - m;
								increasingFactor = Math.max(increasingFactor, 0);

								Double c_2 = 10.0;
								Double h = 0.7;

								Double bucketSizeFilter = c_2 / (currBucket.getSize() + h) + 0.5;

								Double weight = entry.getWeight() * increasingFactor * bucketSizeFilter;
								queryAnalyzer.increaseWeight(idx, weight);
							}
						}
					}
				}
			}
		}

		// Retrieve and format matching entries
		List<ReturnEntry> matchingIndices = queryAnalyzer.getSortedEntries();
		for (ReturnEntry entry : matchingIndices) {
			Integer idx = entry.getIndex();
			Map<String, String> matchingRowData = this.sourceDataFrame.get(idx);
			Double weight = entry.getWeight();
			matchingRowData.put("weight", String.format("%.3f", weight));
			matchingEntries.add(matchingRowData);
		}

		return matchingEntries;
	}

	/**
	 * Retrieves the name of the database.
	 *
	 * @return The name of the database.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the database.
	 *
	 * @param name The new name of the database.
	 */
	public void setName(String name) {
		this.name = name;
	}
}