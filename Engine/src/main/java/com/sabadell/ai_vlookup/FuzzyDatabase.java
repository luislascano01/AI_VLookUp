package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * Represents a fuzzy logic-based database that allows data to be tokenized and
 * indexed for efficient lookup operations. It uses a configurable backbone (via
 * a YAML file) to manage the relationships between data groups and headers
 */

public class FuzzyDatabase implements Serializable {

	private static final long serialVersionUID = 1L;

	private BidirectionalGroupMap backbone; // Backbone configuration for managing headers and groups
	private String name; // Name of the database
	private Dataframe sourceDataFrame; // The source data for the database

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
	 * This method serializes the entire FuzzyDatabase object and writes it to the specified file.
	 * If the parent directories of the file do not exist, they will be created.
	 * If the file does not exist, it will be created automatically. If the file exists, it will be overwritten.
	 * Proper error handling is implemented to catch and log any IO exceptions that may occur during the process.
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

		for (Map<String, String> rowData : df) { // Iterate over all rows
			for (String inputHeader : inputHeaders) { // Process required columns
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
	}
	
	private void cleanPreviousQueryData() {
		
		Map<String, GroupBlock> toClean = this.backbone.getGroups(false);
		
		for(Map.Entry<String, GroupBlock> blockEntry : toClean.entrySet()) {
			
			blockEntry.getValue().setData(null);
			
		}
		
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
		
		ArrayList<Map<String, String>> matchingEntries = new ArrayList<>();
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
					List<String> groupTokens = new ArrayList<>(tokenizedEntry);	
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
								Double weight = entry.getWeight();
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
			matchingRowData.put("Weight", String.format("%.3f", weight));
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