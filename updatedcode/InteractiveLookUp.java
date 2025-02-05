	package com.sabadell.ai_vlookup;
	
	import java.io.File;
	import java.net.URISyntaxException;
	import java.util.ArrayList;
	import java.util.HashMap;
	import java.util.Map;
	
	public class InteractiveLookUp {
		
		
		private static String projectBasePath;
	
		private static String refDataPath = projectBasePath + "/Sample_Dataset/Primary_Reference_Table.xlsx";
		
		private static String operatingDirPath = projectBasePath + "/OperatingDir";
		
		private static FuzzyDatabase database;
		
		public InteractiveLookUp(String projectBasePath) {
			
			this.projectBasePath = projectBasePath;
			
		}
		
		public static void main(String[] args) {
			
			
			String projectBasePath = args[0];
			
			Dataframe referenceData = new Dataframe();
	
			java.net.URL yamlUrl = BidirectionalGroupMap.class.getResource("/header_configuration.yaml");
			if (yamlUrl == null) {
				throw new IllegalStateException("Could not find 'header_configuration.yaml' in resources folder!");
			}
	
			// 2) Convert that URL into a File object
			File yamlFile;
			try {
				yamlFile = new File(yamlUrl.toURI());
			} catch (URISyntaxException e) {
				// fallback if there's something off about the URI
				yamlFile = new File(yamlUrl.getPath());
			}
	
			try {
				referenceData.loadFromExcel(refDataPath, "Primary_Reference_Table", false);
			} catch (Exception e) {
				System.err.println("Error loading reference data");
				e.printStackTrace();
	
			}
			
			database = new FuzzyDatabase("Testing", yamlFile);
			database.loadData(referenceData);
			String databasePath = operatingDirPath + "/database.data";
			database.saveDatabaseToFile(databasePath);
			database = FuzzyDatabase.loadDatabaseFromFile(databasePath);
	
			ArrayList<HashMap<String, String>> data = new ArrayList<>();
	
	
			/*
			HashMap<String, String> row5 = new HashMap<>();
			row5.put("Customer_Name", "Vehement Capital");
			row5.put("Address", "105 Spruce St, Vice City, Florida");
			row5.put("Customer_ID", "");
			data.add(row5);
			*/
			
			/*
			HashMap<String, String> row6 = new HashMap<>();
			row6.put("Customer_Name", "Ecopetrol");
			row6.put("Address", "Calle 37 #8-43, Bogota, Colombia");
			row6.put("Customer_ID", "");
			data.add(row6);
			*/
			
			
			
			HashMap<String, String> row7 = new HashMap<>();
			row7.put("Customer_Name", "Flat Ridge 4 Wind, LLC");
			row7.put("Address", "");
			row7.put("Customer_ID", "");
			row7.put("Industrial_Sector", "Manufacturing");
			data.add(row7);
			
			
	
			for (HashMap<String, String> row : data) {
				ArrayList<Map<String, String>> results = database.lookUpEntry(row); // Call the method and get the results
				if (results != null && !results.isEmpty()) {
					System.out.println("Lookup Results for row:\n" + row + "\n");
					int maxOutputs = 3;
					int i = 0;
					for (Map<String, String> result : results) {
						if(i==maxOutputs) break;
						i++;
						for (Map.Entry<String, String> entry : result.entrySet()) {
							System.out.println(entry.getKey() + ": " + entry.getValue());
						}
						System.out.println(); // Add an empty line for separation between results
					}
				} else {
					System.out.println("Lookup returned no results for row:\n" + row + "\n");
				}
			}
	
		}
	
	}
