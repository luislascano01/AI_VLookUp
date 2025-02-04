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
		this.backboneYamlConfiguration = BidirectionalGroupMap.saveBackboneConfigurationToFile(backboneConfig);

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
	public static Map<String, Object> loadYamlConfiguration(String yamlConfigurationPath) {
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
				boolean sameID = false;
				int topResultIdx = -1;
				int secondTopIdx = -1;
				if (results.size() > 0) {
					List<Map<String, String>> topEntries = new ArrayList<>(
							results.subList(0, Math.min(6, results.size())));
					//topEntries = fuzzyDatabase.sortMatchesBySimilarityToQuery(query, topEntries);
					Map<String, String> topEntry = topEntries.get(0);
					// Map<String, String> topEntry = topEntries.get(0);
					topResultIdx = Integer.parseInt(topEntry.get("index"));

					if (results.size() > 1) {

						String secondTopIdxStr = topEntries.get(1).get("index");
						secondTopIdx = Integer.parseInt(secondTopIdxStr);

					}

					matchingCoefficientDamerau = fuzzyDatabase.compareEntriesDamerau(query, topEntry);
					matchingCoefficientJaccard = fuzzyDatabase.compareEntriesJaccard(query, topEntry);
					sameID = fuzzyDatabase.compareByID(topEntry, query);
				}
				String[] idPair = new String[] { queryIdx + "", topResultIdx + "", secondTopIdx + "",
						matchingCoefficientDamerau + "", matchingCoefficientJaccard + "", sameID + "" };
				resultPairs.add(idPair);

				resultsList.add(query);
			}

			// Optionally: output results to CSV or another format
			String simpleStats = getStats(queryData, regexCounts);
			System.out.println(simpleStats);
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

	private void outputResultsMap(List<String[]> resultPairs) {
		int processedCount = resultPairs.size();

		System.out.format("Total number of 	entries processed: %d", processedCount);

		String outputFilePath = operatingDir + "/results.csv"; // Save results in the operating directory

		try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
			// Writing headers
			writer.println("query,match,secondMatch,coefficientDamerau,coefficientJaccard,idMatch");

			for (String[] resultMap : resultPairs) {
				int source = Integer.parseInt(resultMap[0]);
				int target = Integer.parseInt(resultMap[1]);
				int secondTop = Integer.parseInt(resultMap[2]);
				Double coefficientDamerau = Double.parseDouble(resultMap[3]);
				Double coefficientJaccard = Double.parseDouble(resultMap[4]);
				boolean matchingById = Boolean.parseBoolean(resultMap[5]);
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
