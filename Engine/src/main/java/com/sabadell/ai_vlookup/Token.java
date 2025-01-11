package com.sabadell.ai_vlookup;

import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Token {

    /**
     * Tokenizes the input text by splitting words into overlapping segments (cuts).
     *
     * @param input The text to be tokenized.
     * @return A list of tokens and their segments.
     */
    public static List<String> tokenize(String input) {
        if (input == null || input.trim().isEmpty()) {
            return new ArrayList<>(); // Return empty list for null or empty input
        }

        // Step 1: Preprocess input to remove unnecessary punctuation
        input = preprocess(input);

        // Step 2: Initialize Stanford NLP pipeline
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // Step 3: Create an annotation for the input text
        Annotation annotation = new Annotation(input);
        pipeline.annotate(annotation);

        // Step 4: Extract tokens and generate cuts
        List<String> tokens = new ArrayList<String>();
        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            List<edu.stanford.nlp.ling.CoreLabel> tokenAnnotations = sentence.get(CoreAnnotations.TokensAnnotation.class);
            for (edu.stanford.nlp.ling.CoreLabel tokenAnnotation : tokenAnnotations) {
                String token = tokenAnnotation.word();

                // Filter out invalid tokens
                if (isValidToken(token)) {
                    if (isNumericId(token)) {
                        tokens.add(token); // Keep numeric IDs intact
                    } else {
                        tokens.addAll(generateCuts(token.toLowerCase())); // Generate cuts for words
                    }
                }
            }
        }

        return tokens;
    }

    /**
     * Preprocesses the input text by removing unnecessary punctuation and normalizing spaces.
     *
     * @param input The raw input text.
     * @return The cleaned input text.
     */
    private static String preprocess(String input) {
        // Remove unnecessary punctuation but keep meaningful characters
        return input.replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ").trim();
    }

    /**
     * Checks if a token is a valid word or number.
     *
     * @param token The token to check.
     * @return True if the token is valid, false otherwise.
     */
    private static boolean isValidToken(String token) {
        return token.matches("\\w+"); // Matches words and numbers, excludes punctuation
    }

    /**
     * Checks if a token is a numeric ID (e.g., long client numbers).
     *
     * @param token The token to check.
     * @return True if the token is a numeric ID, false otherwise.
     */
    private static boolean isNumericId(String token) {
        return token.matches("\\d{3,}"); // Matches numbers with 3 or more digits
    }

    /**
     * Generates overlapping cuts (segments) of a word.
     *
     * @param word The word to split.
     * @return A list of overlapping segments.
     */
    private static List<String> generateCuts(String word) {
        List<String> cuts = new ArrayList<String>();
        int cutSize = 4; // The size of each cut (can be adjusted)

        for (int i = 0; i < word.length() - cutSize + 1; i+=2) {
            cuts.add(word.substring(i, i + cutSize)); // Generate a cut of size `cutSize`
        }

        return cuts;
    }

    public static void main(String[] args) {
        // Example client data
        String clientName = "John Doe";
        String clientAddress = "123 Main St, Springfield, IL 62704";
        String clientId = "123456789";

        // Tokenizing each field
        List<String> nameTokens = tokenize(clientName);
        List<String> addressTokens = tokenize(clientAddress);
        List<String> idTokens = tokenize(clientId);

        System.out.println("Name Tokens: " + nameTokens);
        System.out.println("Address Tokens: " + addressTokens);
        System.out.println("ID Tokens: " + idTokens);
    }
}