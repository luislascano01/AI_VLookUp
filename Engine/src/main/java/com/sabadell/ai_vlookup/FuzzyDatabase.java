package com.sabadell.ai_vlookup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class FuzzyDatabase {
	private BidirectionalGroupMap backbone;
	private String name;
	private Dataframe sourceDataFrame;

	public FuzzyDatabase(String name, File yaml) {
		try {
			backbone = new BidirectionalGroupMap(yaml);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Unable to load backbone configuration");
			e.printStackTrace();
		}
		this.name = name;

	}

	public void loadData(Dataframe df) {
		this.sourceDataFrame = df;
		List<String> inputHeaders = this.backbone.getInputHeaders(true);

		for (Map<String, String> rowData : df) { // Iterate over all entries

			for (String inputHeader : inputHeaders) { // Iterate over needed columns

				// Retrieve needed data
				String currCellData = rowData.get(inputHeader);
				String rowIdx = rowData.get("index");

				// Tokenize data
				List<String> tokens = Token.tokenize(currCellData);

				List<GroupBlock> pointedGroups = this.backbone.getGroupsFromHeader(inputHeader, true);
				for (GroupBlock currBlock : pointedGroups) {
					Pool currPool = null;
					if (currBlock.getData() == null) {
						currPool = new Pool();
					} else if (currBlock.getData() instanceof Pool) {
						currPool = (Pool) currBlock.getData();
					} else {
						continue;
					}

					Double weight = currBlock.getHeaderWeight(inputHeader);

					BucketEntry bE = new BucketEntry(Integer.parseInt(rowIdx), weight);

					for (String token : tokens) {
						currPool.placeEntry(token, bE);
					}

					currBlock.setData(currPool);

				}

			}

		}

	}

	public ArrayList<Map<String, String>> lookUpEntry(HashMap<String, String> rowData) {

		ArrayList<Map<String, String>> matchingEntries = new ArrayList<Map<String, String>>();

		List<String> queryHeaders = backbone.getInputHeaders(false);

		QueryAnalyzer qA = new QueryAnalyzer(rowData, queryHeaders);

		// Load tokenized entry into corresponding groups

		for (String header : queryHeaders) {
			List<GroupBlock> targetGroupBlocks = this.backbone.getGroupsFromHeader(header, false);

			List<String> tokenizedEntry = qA.getTokenizedEntryCell(header);

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

		// Aggregate of weight through hash lookup

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

								qA.increaseWeight(idx, weight);

							}
						}
					}

				}

			}

		}
		
		

		List<ReturnEntry> matchingIdxs = qA.getSortedEntries();

		for (ReturnEntry entry : matchingIdxs) {
		    Integer idx = entry.getIndex();
		    Map<String, String> matchingRowData = this.sourceDataFrame.get(idx);
		    Double weight = entry.getWeight();

		    // Format weight to three decimal places
		    matchingRowData.put("Weight", String.format("%.3f", weight));

		    matchingEntries.add(matchingRowData);
		}
		
		return matchingEntries;
	}
	
    private static class Entry {
        private final Integer index;
        private Double weight;

        public Entry(Integer index, Double weight) {
            this.index = index;
            this.weight = weight;
        }

        public Integer getIndex() {
            return index;
        }

        public Double getWeight() {
            return weight;
        }

        public void setWeight(Double weight) {
            this.weight = weight;
        }

        /**
         * Override equals and hashCode so removal by object works based on 'index'.
         * If you need multiple entries with the same 'index' but different weights,
         * you'll have to handle that logic differently (e.g., store them in a list).
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Entry other = (Entry) obj;
            return Objects.equals(this.index, other.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
