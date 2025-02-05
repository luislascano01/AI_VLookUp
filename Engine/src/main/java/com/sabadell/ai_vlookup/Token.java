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
				"Entidad", "S.C.S.", "S.Coop.", "S.A. de C.V.", "Grupo", "Asociacion", "City", // Spanish

				// Custom stop words above the 150 common threshold

				"or", "a", "de", "s", "inc", "maria", "llc", "c", "jose", "corp", "ltd", "l", "v", "luis", "limited",
				"juan", "investments", "antonio", "garcia", "gonzalez", "del", "rodriguez", "carlos", "international",
				"sa", "corporation", "the", "francisco", "manuel", "fernandez", "seafood", "usa", "lopez", "perez",
				"group", "trust", "y", "r", "holdings", "banco", "trading", "la", "investment", "martinez", "jorge",
				"eduardo", "sanchez", "and", "miguel", "alberto", "company", "inversiones", "jesus", "gomez",
				"fernando", "enrique", "m", "carmen", "javier"

		).stream().map(String::toLowerCase).collect(Collectors.toList())); // Normalize stop words to lowercase
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Tokenizes the input text by splitting words into overlapping segments (cuts)
	 * and also extracts word sequences using a sliding window approach.
	 *
	 * @param input The text to be tokenized.
	 * @return A list of tokens and their segments.
	 */
	public static List<String> tokenize(String input) {
		if (input == null || input.trim().isEmpty()) {
			return new ArrayList<>(); // Return empty list for null or empty input
		}

		List<String> tokens = new ArrayList<>();

		input = input.replaceAll("^[\\p{Punct}\\s]+|[\\p{Punct}\\s]+$", "");
		input = input.toLowerCase();

		if (input.contentEquals("")) {
			return tokens;
		}
		input = input.trim().toLowerCase();
		if (input.length() > 10) {
			for (int i = 0; i < 400; i++) {
				tokens.add("$" + input + "$");
			}
		} else if (input.length() > 7) {
			for (int i = 0; i < 100; i++) {
				tokens.add("$" + input + "$");
			}
		}

		// Step 1: Preprocess input to remove unnecessary punctuation
		input = preprocess(input);

		List<String> words = new ArrayList<>(Arrays.asList(input.split("\\s+")));
		words.removeAll(stopWords.get("Common")); // Remove common stop words

		input = String.join(" ", words);

		// Step 4: Extract tokens and generate cuts
		int top = 17;
		for (String token : words) {
			if (token.length() < 2) {
				continue;
			}
			// Filter out invalid tokens
			if (isValidToken(token)) {
				tokens.add("$" + token + "$");
				tokens.add("$#" + token + "$#");

				if (isNumericId(token)) {
					tokens.add(token);
					tokens.add(token);
					tokens.add(token);
				} else {
					tokens.addAll(generateCuts(token, 4));
					tokens.addAll(generateCuts(token, 5));
					tokens.addAll(generateCuts(token, 7));
					tokens.addAll(generateCuts(token, 8));
					tokens.addAll(generateCuts(token, 10));
					tokens.addAll(generateCuts(token, 10));
					tokens.addAll(generateCuts(token, 13));
					tokens.addAll(generateCuts(token, 14));
					tokens.addAll(generateCuts(token, 15));
					tokens.addAll(generateCuts(token, 17));
					tokens.addAll(generateCuts(token, 17));
				}
			}
		}

		// 🔹 ADD SLIDING WINDOW WORD SEQUENCES (NEW ADDITION)
		tokens.addAll(generateWordWindows(words, 2)); // Bi-grams (2-word phrases)
		tokens.addAll(generateWordWindows(words, 3)); // Tri-grams (3-word phrases)
		tokens.addAll(generateWordWindows(words, 4)); // Four-word phrases

		return tokens;
	}

	/**
	 * Generates word sequences using a sliding window approach.
	 *
	 * @param words      The list of words from the input string.
	 * @param windowSize The number of words in each window.
	 * @return A list of tokenized phrases.
	 */
	private static List<String> generateWordWindows(List<String> words, int windowSize) {
		List<String> wordSequences = new ArrayList<>();
		if (words.size() < windowSize)
			return wordSequences; // Skip if not enough words

		for (int i = 0; i <= words.size() - windowSize; i++) {
			wordSequences.add(String.join(" ", words.subList(i, i + windowSize)));
		}
		return wordSequences;
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

		if (cutSize > word.length()) {
			return cuts;
		}

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
	/// Numeric ID Regex: Customer_ID: "((?<![/\\d])\\d+(?![/\\d]))"
}