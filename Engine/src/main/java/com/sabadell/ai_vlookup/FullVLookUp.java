package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import java.io.PrintWriter;
import java.io.FileWriter;

public class FullVLookUp {
	private String referenceTablePath;
	private String queryFilePath;
	private String operatingDir;

	private FuzzyDatabase fuzzyDatabase;
	private Dataframe referenceData;
	private Map<String, Object> configuration;
	private Map<String, String> preProcessRegexMap;

	private File backboneYamlConfiguration;

	private Double topWeightValue = Double.MIN_VALUE;

	/**
	 * Constructs a FullVLookUp instance by loading the YAML configuration from the
	 * given path.
	 *
	 * @param yamlConfigurationPath Path to the YAML configuration file.
	 */
	public FullVLookUp(String yamlConfigurationPath) {
		this.referenceData = new Dataframe();

		// Load YAML configuration
		this.configuration = loadYamlConfiguration(yamlConfigurationPath);

		// Extract data paths and backbone configuration from YAML
		Map<String, Object> fuzzyDatabaseConfig = (Map<String, Object>) configuration.get("FuzzyDatabaseConfig");

		if (fuzzyDatabaseConfig == null) {
			throw new IllegalArgumentException("Missing 'FuzzyDatabaseConfig' in YAML configuration.");
		}

		// Extract BackboneConfiguration
		Map<String, Object> backboneConfig = (Map<String, Object>) fuzzyDatabaseConfig.get("BackboneConfiguration");
		if (backboneConfig == null) {
			throw new IllegalArgumentException("Missing 'BackboneConfiguration' in YAML configuration.");
		}

		// Save BackboneConfiguration to a temporary file
		this.backboneYamlConfiguration = saveBackboneConfigurationToFile(backboneConfig);

		// Extract data paths
		Map<String, String> dataToConsume = (Map<String, String>) fuzzyDatabaseConfig.get("DataToConsume");
		if (dataToConsume == null) {
			throw new IllegalArgumentException("Missing 'DataToConsume' in YAML configuration.");
		}

		this.referenceTablePath = dataToConsume.get("ReferenceTable");
		this.queryFilePath = dataToConsume.get("MessyTable");

		if (referenceTablePath == null || queryFilePath == null) {
			throw new IllegalArgumentException(
					"Missing data paths (ReferenceTable or MessyTable) in YAML configuration.");
		}

		// Extract operating directory
		this.operatingDir = (String) fuzzyDatabaseConfig.get("OperatingDir");
		if (operatingDir == null) {
			throw new IllegalArgumentException("Missing 'OperatingDir' in YAML configuration.");
		}

		// Extract RegexPreprocessing
		this.preProcessRegexMap = (Map<String, String>) fuzzyDatabaseConfig.get("RegexPreprocessing");
		if (preProcessRegexMap == null) {
			System.out.println(
					"Warning: 'RegexPreprocessing' not defined in YAML configuration. Skipping preprocessing.");
			preProcessRegexMap = new HashMap<>(); // Initialize as empty to avoid null references
		}

		// Initialize the FuzzyDatabase with the backbone configuration
		this.fuzzyDatabase = new FuzzyDatabase("FullQueryDB", backboneYamlConfiguration);
	}

	/**
	 * Loads the YAML configuration from the given file path.
	 *
	 * @param yamlConfigurationPath Path to the YAML configuration file.
	 * @return A Map representing the parsed YAML content.
	 */
	private static Map<String, Object> loadYamlConfiguration(String yamlConfigurationPath) {
		try {
			File yamlFile = new File(yamlConfigurationPath);
			if (!yamlFile.exists() || !yamlFile.isFile()) {
				throw new IllegalArgumentException("YAML configuration file not found: " + yamlConfigurationPath);
			}

			Yaml yaml = new Yaml();
			return yaml.load(new java.io.FileReader(yamlFile));
		} catch (Exception e) {
			System.err.println("Failed to load YAML configuration: " + e.getMessage());
			throw new RuntimeException("Error loading YAML configuration from path: " + yamlConfigurationPath, e);
		}
	}

	/**
	 * Saves the backbone configuration section to a temporary YAML file.
	 *
	 * @param backboneConfig The backbone configuration to save.
	 * @return A File object pointing to the saved YAML file.
	 */
	private File saveBackboneConfigurationToFile(Map<String, Object> backboneConfig) {
		try {
			File tempFile = File.createTempFile("backbone_configuration", ".yaml");
			tempFile.deleteOnExit(); // Automatically delete the file on JVM exit

			Yaml yaml = new Yaml();
			try (FileWriter writer = new FileWriter(tempFile)) {
				yaml.dump(backboneConfig, writer);
			}

			return tempFile;
		} catch (IOException e) {
			System.err.println("Failed to save backbone configuration to file: " + e.getMessage());
			throw new RuntimeException("Error saving backbone configuration to temporary file.", e);
		}
	}

	/**
	 * Initializes the reference data and loads it into the FuzzyDatabase.
	 */
	public void initialize() {
		try {
			// Load reference data from an Excel file into the Dataframe
			referenceData.loadFromExcel(referenceTablePath, "Primary_Reference_Table", false);
			// referenceData.preProcessRegex(this.preProcessRegexMap); // Functionality here

			// Load reference data into FuzzyDatabase
			fuzzyDatabase.loadData(referenceData);
		} catch (IOException e) {
			System.err.println("Error loading reference data: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Executes queries and processes results .
	 */
	public void performQueries() {
		Dataframe queryData = new Dataframe();
		try {
			// Load query data from a CSV file
			queryData.loadFromExcel(queryFilePath, "Secondary_Data", false);

			Map<String, Integer> regexCounts = queryData.preProcessRegex(this.preProcessRegexMap); // Functionality here

			List<Map<String, String>> resultsList = new ArrayList<Map<String, String>>();
			List<String[]> resultPairs = new ArrayList<String[]>();

			// Perform query for each row in the query Dataframe
			for (Map<String, String> query : queryData) {
				// int queryVerifyID = Integer.parseInt(query.get("Verify_ID"));
				int queryIdx = Integer.parseInt(query.get("index"));
				ArrayList<Map<String, String>> results = fuzzyDatabase.lookUpEntry(query);
				Double matchingCoefficientDamerau = -1.0;
				Double matchingCoefficientJaccard = -1.0;
				String topResultWeight = "-1";
				String secondTopWeight = "-1";
				boolean sameID = false;
				int topResultIdx = -1;
				int secondTopIdx = -1;
				if (results.size() > 0) {
					List<Map<String, String>> topEntries = new ArrayList<>(
							results.subList(0, Math.min(6, results.size())));
					// topEntries = fuzzyDatabase.sortMatchesBySimilarityToQuery(query, topEntries);
					Map<String, String> topEntry = topEntries.get(0);
					// Map<String, String> topEntry = topEntries.get(0);
					topResultIdx = Integer.parseInt(topEntry.get("index"));

					topResultWeight = topEntry.get("weight");

					Double currWeight = Double.parseDouble(secondTopWeight);

					this.topWeightValue = Math.max(this.topWeightValue, currWeight);

					if (results.size() > 1) {

						String secondTopIdxStr = topEntries.get(1).get("index");
						secondTopIdx = Integer.parseInt(secondTopIdxStr);
						secondTopWeight = topEntries.get(1).get("weight");
						Double secondTW = Double.parseDouble(secondTopWeight);
						this.topWeightValue = Math.max(this.topWeightValue, secondTW);

					}

					matchingCoefficientDamerau = fuzzyDatabase.compareEntriesDamerau(query, topEntry);
					matchingCoefficientJaccard = fuzzyDatabase.compareEntriesJaccard(query, topEntry);
					sameID = fuzzyDatabase.compareByID(topEntry, query);
				}
				String[] idPair = new String[] { queryIdx + "", topResultIdx + "", topResultWeight + "",
						secondTopIdx + "", secondTopWeight + "", matchingCoefficientDamerau + "",
						matchingCoefficientJaccard + "", sameID + "" };
				resultPairs.add(idPair);

				resultsList.add(query);
			}

			// Optionally: output results to CSV or another format
			String simpleStats = getStats(queryData, regexCounts);
			System.out.println(simpleStats);

			// Code for re arranging ranking based on overlapping matches. We use a re
			// arranger class
			// that leverages a HashMap to find collisions between topResultIdx; then
			// between those collisions
			// the algorithm should perform a Lenvenshtein-Swap analysis between the
			// corresponding idx in QueryData
			// and whichever colliding entry has the lowest edit distance gets assigned the
			// match and gets
			// to keep the idx as topResultIdx; for the loosing colliding entry we proceed
			// to move up the secondTopIdx
			// into the topResultIdx, leaving secondTopIdx as -1.
			// Every time a secondMatchMove up; it should be added again to the HashMap
			// backed collision checking
			// data structure as part of algorithm completeness.
			// Now, this case described is only for the top two matches; however, you should
			// follow generabilization
			// principles, therefore structure the algorithm in such way that no matter how
			// many ranks there are (in our case two)
			// the algorithm can still execute through all ranks and find the ultimate
			// arrangement in such way that there are no avoidable collisions
			// Also, the top idx should never be -1 if it had a number previously; meaning
			// that if there are two colliding idxs and there is nothing in the
			// ranks to move up; then they'll stay colliding.
			
			
			//// Part Two.

			// The description above is already working; however, we want to improve the
			
			
			// algorithm, I need to modify the method rearrageCollisions. The current
			// problem that I'm having
			// is that if a entry has a secondMatcht that does not necessarily meean that
			// second match
			// is worth moving up; therefore I want to introduce a method hyperparameter
			// (diffPercent) threshold.

			// How this will work is that the Levenshtein distance of the new match going up
			// can only
			// be diffPercent (in percentage relative to distance of main match) higher than
			// the current match. Otherwise the current match stays and the become verified colliding
			// matches.
			// CollisionRearranger.rearrangeCollisions(queryData, resultPairs, diffPercent);

			// After you've built resultPairs:
			double diffPercent = 0.05; // threshold
			CollisionRearranger.rearrangeCollisions(queryData, resultPairs, diffPercent);

			outputResultsMap(resultPairs);

		} catch (IOException e) {
			System.err.println("Failed to load query CSV: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private String getStats(Dataframe queryData, Map<String, Integer> regexCounts) {
		StringBuilder statText = new StringBuilder("############  Stats  ############\n\n");
		statText.append("General Stats:\n");

		int totalEntries = queryData.size();
		statText.append(String.format("Total entries: %d \n\n", totalEntries));

		statText.append("Regex Header Stats:\n");

		// Outer border top line
		statText.append("+---------------------+--------+------------+\n");
		statText.append(String.format("| %-20s | %-6s | %-10s |\n", "Header", "Count", "Percentage"));
		statText.append("+---------------------+--------+------------+\n");

		for (Map.Entry<String, Integer> entry : regexCounts.entrySet()) {
			double percentage = (entry.getValue() * 100.0) / totalEntries;
			// Adjusted the percentage format to ensure it does not include unwanted space
			// or characters.
			statText.append(
					String.format("| %-20s | %-6d | %9.2f%% |\n", entry.getKey(), entry.getValue(), percentage));
		}

		// Outer border bottom line
		statText.append("+---------------------+--------+------------+\n");

		return statText.toString();
	}

	private void outputResultsMap(List<String[]> resultTuples) {
		int processedCount = resultTuples.size();

		System.out.format("Total number of 	entries processed: %d", processedCount);

		String outputFilePath = operatingDir + "/results.csv"; // Save results in the operating directory

		try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
			// Writing headers
			writer.println("query,match,secondMatch,coefficientDamerau,coefficientJaccard,idMatch");
			/*
			 * Double sumExpWeight = 1e-10; for (String[] resultMap : resultTuples) { Double
			 * targetW = Double.parseDouble(resultMap[2]); if (targetW > 0) sumExpWeight +=
			 * Math.exp(targetW - this.topWeightValue); Double secondW =
			 * Double.parseDouble(resultMap[4]); if (secondW > 0) sumExpWeight +=
			 * Math.exp(secondW - this.topWeightValue);
			 * 
			 * }
			 */
			for (String[] resultMap : resultTuples) {
				int source = Integer.parseInt(resultMap[0]);
				int target = Integer.parseInt(resultMap[1]);
				// Double targetW = Double.parseDouble(resultMap[2]);
				int secondTop = Integer.parseInt(resultMap[3]);

				/*
				 * Double targetWShift = 0.0; Double normalizedTW = -1.0; if (targetW > 0) {
				 * targetWShift = targetW - this.topWeightValue; normalizedTW =
				 * Math.exp(targetWShift) / sumExpWeight; }
				 * 
				 * 
				 * 
				 * Double secondWShift = 0.0; Double normalizedSW = -1.0; if (secondW > 0) {
				 * secondWShift = secondW - this.topWeightValue; normalizedSW =
				 * Math.exp(secondWShift) / sumExpWeight; }
				 */
				Double coefficientDamerau = Double.parseDouble(resultMap[5]);
				Double coefficientJaccard = Double.parseDouble(resultMap[6]);
				boolean matchingById = Boolean.parseBoolean(resultMap[7]);
				writer.printf("%d,%d,%d,%.3f,%.3f,%d\n", source, target, secondTop, coefficientDamerau,
						coefficientJaccard, matchingById ? 1 : 0);
			}
		} catch (IOException e) {
			System.err.println("Failed to write results to CSV: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: FullVLookUp <yamlConfigurationPath>");
			return;
		}

		FullVLookUp lookup = new FullVLookUp(args[0]);
		lookup.initialize();
		lookup.performQueries();
	}
}
