package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
                "Missing data paths (ReferenceTable or MessyTable) in YAML configuration."
            );
        }

        // Extract operating directory
        this.operatingDir = (String) fuzzyDatabaseConfig.get("OperatingDir");
        if (operatingDir == null) {
            throw new IllegalArgumentException("Missing 'OperatingDir' in YAML configuration.");
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

            // Load reference data into FuzzyDatabase
            fuzzyDatabase.loadData(referenceData);
        } catch (IOException e) {
            System.err.println("Error loading reference data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Executes queries and processes results.
     */
    public void performQueries() {
        Dataframe queryData = new Dataframe();
        try {
            // Load query data from a CSV file
            queryData.loadFromExcel(queryFilePath, "Secondary_Data", false);

            List<Map<String, String>> resultsList = new ArrayList<>();
            List<Integer[]> resultPairs = new ArrayList<>();

            // Perform query for each row in the query Dataframe
            for (Map<String, String> query : queryData) {
                int queryVerifyID = Integer.parseInt(query.get("Verify_ID"));
                ArrayList<Map<String, String>> results = fuzzyDatabase.lookUpEntry(query);

                int topResultIdx = Integer.parseInt(results.get(0).get("Customer_ID"));

                Integer[] idPair = new Integer[]{queryVerifyID, topResultIdx};

                resultPairs.add(idPair);

                processResults(query, results);
                resultsList.add(query);
            }

            // Optionally: output results to CSV or another format
            outputResults(resultPairs);
        } catch (IOException e) {
            System.err.println("Failed to load query CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processResults(Map<String, String> query, ArrayList<Map<String, String>> results) {
        if (results.isEmpty()) {
            System.out.println("No results found for: " + query);
        } else {
            System.out.println("Results for " + query + ":");
            results.forEach(result -> {
                result.forEach((key, value) -> System.out.println(key + ": " + value));
                System.out.println();
            });
        }
    }

    private void outputResults(List<Integer[]> resultPairs) {
        int processedCount = resultPairs.size();

        System.out.format("Total number of entries processed: %d", processedCount);

        String outputFilePath = operatingDir + "/results.csv"; // Save results in the operating directory

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            // Writing headers
            writer.println("source,target");

            for (Integer[] resultMap : resultPairs) {
                Integer source = resultMap[0];
                Integer target = resultMap[1];
                writer.printf("%d,%d\n", source, target);
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