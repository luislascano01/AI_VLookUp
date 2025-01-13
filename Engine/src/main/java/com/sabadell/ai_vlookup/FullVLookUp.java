package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.PrintWriter;
import java.io.FileWriter;

public class FullVLookUp {
    private String referenceTablePath;
    private String queryFilePath;
    private FuzzyDatabase fuzzyDatabase;
    private Dataframe referenceData;

    public FullVLookUp(String referenceTablePath, String queryFilePath) {
        this.referenceTablePath = referenceTablePath;
        this.queryFilePath = queryFilePath;
        this.referenceData = new Dataframe();

        // Load YAML configuration for the FuzzyDatabase
        File yamlFile = loadYamlConfiguration();
        this.fuzzyDatabase = new FuzzyDatabase("FullQueryDB", yamlFile);
    }

    private File loadYamlConfiguration() {
        try {
            java.net.URL yamlUrl = FullVLookUp.class.getResource("/header_configuration.yaml");
            if (yamlUrl == null) {
                throw new IllegalStateException("Could not find 'header_configuration.yaml' in resources folder!");
            }
            try {
                return new File(yamlUrl.toURI());
            } catch (URISyntaxException e) {
                return new File(yamlUrl.getPath());
            }
        } catch (Exception e) {
            System.err.println("Failed to load YAML configuration: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void initialize() {
        try {
            // Load reference data from an Excel file into the Dataframe
            referenceData.loadFromExcel(referenceTablePath, "Primary_Reference_Table", false);
            // Assuming FuzzyDatabase is correctly set up to take Dataframe and use it
            fuzzyDatabase.loadData(referenceData);
        } catch (IOException e) {
            System.err.println("Error loading reference data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void performQueries() {
        Dataframe queryData = new Dataframe();
        try {
            // Load query data from a CSV file
            queryData.loadFromExcel(queryFilePath, "Secondary_Data", false);
            
            List<Map<String, String>> resultsList = new ArrayList<>();
            List<Integer[]> resultPairs = new ArrayList<Integer[]>();
            
            // Perform query for each row in the query Dataframe
            for (Map<String, String> query : queryData) {
            	int queryVerifyID = Integer.parseInt(query.get("Verify_ID"));
                ArrayList<Map<String, String>> results = fuzzyDatabase.lookUpEntry(query);
                
                int topResultIdx = Integer.parseInt(results.get(0).get("Customer_ID"));
                
                Integer[] idPair = new Integer[] {queryVerifyID, topResultIdx};
                
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
    	
    	String outputFilePath = "/Users/luislascano01/Documents/Sabadell/Covenants_Matching/AI_VLookUp/OperatingDir/results.csv"; // Specify your output file path

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {
            // Writing headers
            writer.println("source,target");

            // Assuming each result map in resultsList is structured in a way that
            // "source" and "target" keys exist and correspond to your desired output
            for (Integer[] resultMap : resultPairs) {
                Integer source = resultMap[0]; // Replace "source" with your actual key
                Integer target = resultMap[1]; // Replace "target" with your actual key
                writer.printf("%d,%d\n", source, target);
            }
        } catch (IOException e) {
            System.err.println("Failed to write results to CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: FullVLookUp <referenceTablePath> <queryFilePath>");
            return;
        }

        FullVLookUp lookup = new FullVLookUp(args[0], args[1]);
        lookup.initialize();
        lookup.performQueries();
    }
}