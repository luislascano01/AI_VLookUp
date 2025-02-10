package com.sabadell.ai_vlookup;

import java.util.*;
import java.util.stream.Collectors;

public class CollisionRearranger {

    /**
     * Rearranges collisions among query rows that share the same topResultIdx,
     * allowing secondTopIdx to "promote" ONLY IF its distance is within 
     * 'diffPercent' threshold relative to the winning distance.
     * 
     * @param queryData    The Dataframe (for computing Levenshtein distance).
     * @param resultPairs  A List of String[] representing each query's match data:
     *                     [   
     *                       0 => queryIdx
     *                       1 => topResultIdx
     *                       2 => topResultWeight       (optional/unused here)
     *                       3 => secondTopIdx
     *                       4 => secondTopWeight       (optional/unused here)
     *                       5 => coefficientDamerau    (optional)
     *                       6 => coefficientJaccard    (optional)
     *                       7 => sameID                (optional)
     *                       8 => verifiedCollision?    (new marker; can be null/unused)
     *                     ]
     * @param diffPercent  A decimal, e.g. 0.20 means "second match can be up to 20%
     *                     worse than the winner's distance" to allow promotion.
     */
    public static void rearrangeCollisions(Dataframe queryData,
                                           List<String[]> resultPairs,
                                           double diffPercent) 
    {
        // 1. Process collisions repeatedly until no more changes occur
        boolean changed;
        do {
            changed = false;

            // 1.1 Build a map of topResultIdx -> list of row indices that have that topResultIdx
            Map<Integer, List<Integer>> collisionMap = buildCollisionMap(resultPairs);

            for (Map.Entry<Integer, List<Integer>> entry : collisionMap.entrySet()) {
                int topResult = entry.getKey();
                List<Integer> collisions = entry.getValue();

                // If only 1 query claims this topResult, no collision to resolve
                if (collisions.size() < 2) {
                    continue;
                }

                // Among the collisions, pick the single "winner"
                // by computing the best (lowest) Levenshtein distance
                int bestQueryRowIndex = pickBestCollision(queryData, resultPairs, collisions);

                // Everyone else tries to promote secondTopIdx IF within threshold
                for (int rowIndex : collisions) {
                    if (rowIndex == bestQueryRowIndex) {
                        continue; // The winner stays top
                    }
                    // Possibly skip if we've already "verified" this collision
                    if (isVerifiedCollision(resultPairs.get(rowIndex))) {
                        continue; // no further promotions
                    }

                    boolean updated = tryPromoteSecondMatch(queryData, resultPairs, rowIndex,
                                                            bestQueryRowIndex, diffPercent);
                    changed = changed || updated;
                }
            }
        } while (changed);
    }

    /**
     * Build a map of topResultIdx -> list of row indices in resultPairs that share
     * that topResultIdx, ignoring any row marked "verifiedCollision".
     */
    private static Map<Integer, List<Integer>> buildCollisionMap(List<String[]> resultPairs) {
        Map<Integer, List<Integer>> collisionMap = new HashMap<>();
        for (int i = 0; i < resultPairs.size(); i++) {
            String[] row = resultPairs.get(i);
            // If row is marked as verified collision, skip adding it to the map
            if (isVerifiedCollision(row)) {
                continue;
            }

            int topResultIdx = Integer.parseInt(row[1]);
            collisionMap
                .computeIfAbsent(topResultIdx, k -> new ArrayList<>())
                .add(i);
        }
        return collisionMap;
    }

    /**
     * Among multiple queries that share the same topResultIdx,
     * pick the one with the smallest Levenshtein distance
     * to the matching record.
     */
    private static int pickBestCollision(Dataframe queryData, 
                                         List<String[]> resultPairs,
                                         List<Integer> collisions) 
    {
        int bestIndex = collisions.get(0);
        double bestDistance = Double.MAX_VALUE;

        for (int rowIndex : collisions) {
            String[] row = resultPairs.get(rowIndex);
            if (isVerifiedCollision(row)) {
                // skip verified collisions from "winning"
                continue;
            }
            int queryIdx = Integer.parseInt(row[0]);
            int topResultIdx = Integer.parseInt(row[1]);

            double distance = computeLevenshteinForCollision(queryData, queryIdx, topResultIdx);
            if (distance < bestDistance) {
                bestDistance = distance;
                bestIndex = rowIndex;
            }
        }
        return bestIndex;
    }

    /**
     * Attempt to promote secondTopIdx if it is within the 'diffPercent' threshold
     * relative to the winning distance. If the second match is too far off,
     * we mark this row as a "verified collision" and do NOT promote.
     *
     * @param rowIndex        Index in resultPairs of the losing collision
     * @param bestQueryRowIdx Index in resultPairs of the winner
     * @return true if promotion occurred, false otherwise
     */
    private static boolean tryPromoteSecondMatch(Dataframe queryData,
                                                 List<String[]> resultPairs,
                                                 int rowIndex,
                                                 int bestQueryRowIdx,
                                                 double diffPercent)
    {
        String[] loserRow = resultPairs.get(rowIndex);
        int secondTopIdx = Integer.parseInt(loserRow[3]);
        if (secondTopIdx < 0) {
            // No valid secondTop => can't promote => remain colliding
            markVerifiedCollision(loserRow);
            return false;
        }

        // Calculate distance of the winner
        String[] winnerRow = resultPairs.get(bestQueryRowIdx);
        int bestQueryIdx = Integer.parseInt(winnerRow[0]);
        int bestTopIdx = Integer.parseInt(winnerRow[1]);
        double distWinner = computeLevenshteinForCollision(queryData, bestQueryIdx, bestTopIdx);

        // Calculate distance of the potential second match
        int loserQueryIdx = Integer.parseInt(loserRow[0]);
        double distSecond = computeLevenshteinForCollision(queryData, loserQueryIdx, secondTopIdx);

        // Condition:
        //   distSecond <= distWinner * (1 + diffPercent)
        // If true => promote
        // else => remain collision
        if (distSecond <= distWinner * (1 + diffPercent)) {
            // "Promote" secondTopIdx to top
            loserRow[1] = String.valueOf(secondTopIdx);
            // Optionally set secondTopIdx to -1
            loserRow[3] = "-1";
            return true;
        } else {
            // Mark as verified => won't attempt to promote again
            markVerifiedCollision(loserRow);
            return false;
        }
    }

    /**
     * Basic check if row[8] (or any chosen slot) is set to "VERIFIED".
     */
    private static boolean isVerifiedCollision(String[] row) {
        if (row.length > 8 && row[8] != null && row[8].equals("VERIFIED")) {
            return true;
        }
        return false;
    }

    /**
     * Mark the row as "VERIFIED" to skip it in future iterations.
     */
    private static void markVerifiedCollision(String[] row) {
        if (row.length > 8) {
            row[8] = "VERIFIED";
        }
        // Or if row[8] doesn't exist, you can re-size the array or store in a Map
        // For a quick hack: if the array isn't large enough, do nothing or create
        // an extra structure to mark collisions.
    }

    /**
     * Computes the Levenshtein distance between the 'queryIdx' row and 'topResultIdx' row.
     * Adjust as needed for referencing referenceData vs. queryData.
     */
    private static double computeLevenshteinForCollision(Dataframe queryData, int queryIdx, int topResultIdx) {
        Map<String, String> queryRow = queryData.get(queryIdx);
        Map<String, String> otherRow = queryData.get(topResultIdx);

        String qName = findNameColumnValue(queryRow);
        String tName = findNameColumnValue(otherRow);

        return levenshteinDistance(qName, tName);
    }

    /**
     * Find a column containing "name" in the row.
     */
    private static String findNameColumnValue(Map<String, String> row) {
        for (String col : row.keySet()) {
            if (col.toLowerCase().contains("name")) {
                return row.get(col);
            }
        }
        return "";
    }

    /**
     * Simple Levenshtein distance.
     */
    private static int levenshteinDistance(String s1, String s2) {
        if (s1 == null) s1 = "";
        if (s2 == null) s2 = "";

        int len1 = s1.length();
        int len2 = s2.length();
        int[][] dp = new int[len1 + 1][len2 + 1];

        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }

        for (int i = 1; i <= len1; i++) {
            for (int j = 1; j <= len2; j++) {
                int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
                dp[i][j] = Math.min(
                    Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                    dp[i - 1][j - 1] + cost
                );
            }
        }
        return dp[len1][len2];
    }
}