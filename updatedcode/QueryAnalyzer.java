package com.sabadell.ai_vlookup;

import java.util.*;

public class QueryAnalyzer {
    private final Map<String, List<String>> tokenizedEntry;
    private final PriorityQueue<ReturnEntry> matchingEntries;

    // Maps an index -> Entry, so we can quickly locate the Entry
    private final Map<Integer, ReturnEntry> entryMap;

    public QueryAnalyzer(Map<String, String> rawEntry, List<String> inputHeaders) {
        this.tokenizedEntry = new HashMap<>();
        for (String header : inputHeaders) {
            String currRawCell = rawEntry.get(header);
            List<String> tokenizedCell = Token.tokenize(currRawCell);
            tokenizedEntry.put(header, tokenizedCell);
        }

        // PriorityQueue that sorts by weight descending
        this.matchingEntries = new PriorityQueue<ReturnEntry>(Comparator.comparingDouble(ReturnEntry::getWeight).reversed());

        // Map to store the references to each Entry by index
        this.entryMap = new HashMap<Integer, ReturnEntry>();
    }

    public Map<String, List<String>> getTokenizedEntry() {
        return this.tokenizedEntry;
    }

    public List<String> getTokenizedEntryCell(String header) {
        return this.tokenizedEntry.get(header);
    }

    /**
     * Increase the weight for the given idx by the specified amount.
     * If the Entry doesn't exist, create it; otherwise remove it from the heap,
     * update its weight, and re-add it to the heap.
     */
    public void increaseWeight(Integer idx, Double weightToAdd) {
        // Look up the Entry in our map
    	ReturnEntry existing = entryMap.get(idx);

        if (existing != null) {
            // 1. Remove it from the heap
            matchingEntries.remove(existing);

            // 2. Update its weight
            existing.setWeight(existing.getWeight() + weightToAdd);

            // 3. Reinsert into the heap
            matchingEntries.add(existing);
            
            entryMap.put(idx, existing);

        } else {
            // Create a new Entry if we don't already have one for this idx
        	ReturnEntry newEntry = new ReturnEntry(idx, weightToAdd);
            matchingEntries.add(newEntry);
            entryMap.put(idx, newEntry);
        }
    }

    // Method to get entries sorted by weight (removes them from the PriorityQueue)
    public List<ReturnEntry> getSortedEntries() {
        List<ReturnEntry> sortedEntries = new ArrayList<>();
        while (!matchingEntries.isEmpty()) {
        	ReturnEntry e = matchingEntries.poll();
            // Also remove from the map if you want the map to reflect only what's in the queue
            entryMap.remove(e.getIndex());
            sortedEntries.add(e);
        }
        return sortedEntries;
    }

    // Method to return formatted string of sorted entries (also removes them from the PriorityQueue)
    public String getFormattedEntries() {
        StringBuilder sb = new StringBuilder();
        List<ReturnEntry> sortedEntries = getSortedEntries();
        for (ReturnEntry entry : sortedEntries) {
            sb.append(String.format("Index: %d, Weight: %.2f%n", entry.getIndex(), entry.getWeight()));
        }
        return sb.toString();
    }

    // Method to return only the indexes in descending order, without clearing the original queue
    public List<Integer> getSortedIndexes() {
        // Make a copy so the original queue isn't emptied
        PriorityQueue<ReturnEntry> tempQueue = new PriorityQueue<>(matchingEntries);
        List<Integer> result = new ArrayList<>();
        while (!tempQueue.isEmpty()) {
            result.add(tempQueue.poll().getIndex());
        }
        return result;
    }

  
}