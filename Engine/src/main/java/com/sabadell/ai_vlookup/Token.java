package com.sabadell.ai_vlookup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Token implements Serializable {

	// Declare the dictionary as static and final

	public static final HashMap<String, List<String>> stopWords = new HashMap<>();

	static {
		stopWords.put("Common", Arrays.asList("Incorporated", "Corporation", "Company", "Limited", "LLC", "Inc.",
				"Corp.", "Co.", "Ltd.", "LLP", "LP", "L.P.", "L.L.P.", "P.C.", "PLLC", "Corp", "Inc", "Partnership",
				"Group", "Association", "Holdings", "Enterprise", "Enterprises", "Firm", "Trust", "Foundation",
				"Institution", "Organization", "Estate", "Union", "Consortium", "Joint Venture", "JV", "Venture",
				"LLC.",

				// Spanish Stop Words for Business Entities
				"Sociedad Anonima", "Compania Limitada", "Sociedad de Responsabilidad Limitada", "Sociedad Limitada",
				"S.L.", "S.A.", "SA", "S.L.N.E.", "S.R.L.", "S.A.S.", "S. de R.L.", "S.C.", "S.C.S.", "S.Coop.",
				"S.A. de C.V.", "Grupo", "Asociacion", "Fundacion", "Union", "Cooperativa", "Corporacion", "Compania",
				"Negocios", "Empresa", "Empresas", "Comercio", "Sociedad", "Fideicomiso", "Consorcio", "Alianza",
				"Entidad", "S.C.S.", "S.Coop.", "S.A. de C.V.", "Grupo", "Asociacion", "City" // Spanish
		).stream().map(String::toLowerCase).collect(Collectors.toList())); // Normalize stop words to lowercase
	}

	private static final long serialVersionUID = 1L;

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

		List<String> words = new ArrayList<>(Arrays.asList(input.split("\\s+")));
		words.removeAll(stopWords.get("Common")); // Remove common stop words

		// Step 2: Initialize Stanford NLP pipeline
		// Reconstruct input without stop words
		input = String.join(" ", words);

		// Step 4: Extract tokens and generate cuts
		List<String> tokens = new ArrayList<String>();

		for (String token : input.split(" ")) {
			// Filter out invalid tokens
			if (isValidToken(token)) {
				if (isNumericId(token)) {
					// tokens.add(token);
					tokens.add(token);
					tokens.add(token);
					tokens.add(token);
					// tokens.add(token);// Keep numeric IDs intact
				} else {
					// tokens.addAll(generateCuts(token.toLowerCase(), 3));
					tokens.addAll(generateCuts(token, 4));
					// tokens.addAll(generateCuts(token, 5));
					tokens.addAll(generateCuts(token, 8));
					tokens.addAll(generateCuts(token, 10));
					tokens.addAll(generateCuts(token, 13));
					tokens.addAll(generateCuts(token, 13));
					// tokens.addAll(generateCuts(token, 15));
					tokens.addAll(generateCuts(token, 17));
				}
			}
		}

		return tokens;
	}

	/**
	 * Preprocesses the input text by removing unnecessary punctuation and
	 * normalizing spaces.
	 *
	 * @param input The raw input text.
	 * @return The cleaned input text.
	 */
	private static String preprocess(String input) {
		// Remove unnecessary punctuation but keep meaningful characters
		input = input.toLowerCase();
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
		return token.matches("\\d{4,}"); // Matches numbers with 4 or more digits
	}

	/**
	 * Generates overlapping cuts (segments) of a word.
	 *
	 * @param word The word to split.
	 * @return A list of overlapping segments.
	 */
	private static List<String> generateCuts(String word, int cutSize) {
		List<String> cuts = new ArrayList<String>();

		for (int i = 0; i < word.length() - cutSize + 1; i += 2) {
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