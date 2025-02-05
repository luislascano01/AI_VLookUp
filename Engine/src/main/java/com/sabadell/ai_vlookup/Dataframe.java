package com.sabadell.ai_vlookup;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Dataframe that loads rows from an Excel sheet. Each row is stored as a
 * Map<String, String> of columnName -> cellValue. An index column can be
 * included explicitly or generated automatically.
 *
 * This class is Iterable and indexable: - for-each loop usage via the
 * iterator() - get(i) to retrieve the i-th row
 */
public class Dataframe implements Iterable<Map<String, String>>, Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Internally holds one Map per row: rowData = { "index": "0", "Column A": "some
	 * value", ... }
	 */
	public final List<Map<String, String>> data;

	/**
	 * List of column labels, in the order they appear in the sheet.
	 */
	private List<String> columnLabels;

	/**
	 * Constructor initializes an empty Dataframe.
	 */
	public Dataframe() {
		this.data = new ArrayList<>();
		this.columnLabels = new ArrayList<>();
	}

	/**
	 * Loads data from an Excel sheet into the Dataframe.
	 *
	 * @param filePath           Path to the Excel file.
	 * @param sheetName          Name of the sheet to load.
	 * @param isFirstColumnIndex Whether the first column is the index column.
	 * @throws IOException If there is an issue reading the file.
	 */
	public void loadFromExcel(String filePath, String sheetName, boolean isFirstColumnIndex) throws IOException {
		try (FileInputStream fileInputStream = new FileInputStream(filePath);
				Workbook workbook = new XSSFWorkbook(fileInputStream)) {
			Sheet sheet = workbook.getSheet(sheetName);

			if (sheet == null) {
				throw new IllegalArgumentException("Sheet " + sheetName + " does not exist.");
			}

			// Extract column labels from the header row
			this.columnLabels = extractColumnLabels(sheet.getRow(0), isFirstColumnIndex);

			// Add "index" column if the first column is not the index
			if (!isFirstColumnIndex) {
				this.columnLabels.add(0, "index");
			}

			// Load the data rows
			this.data.clear(); // Clear any existing data
			for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
				Row row = sheet.getRow(rowIndex);
				if (row == null)
					continue; // Skip empty rows

				Map<String, String> rowData = extractRowData(row, rowIndex - 1, isFirstColumnIndex);
				this.data.add(rowData);
			}
		}
	}

	/**
	 * Loads data from a CSV file into the Dataframe.
	 *
	 * @param filePath   Path to the CSV file.
	 * @param hasHeaders Whether the CSV file contains column headers.
	 * @throws IOException If there is an issue reading the file.
	 */
	public void loadFromCSV(String filePath, boolean hasHeaders) throws IOException {
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			this.data.clear();
			this.columnLabels.clear();

			String line;
			int rowIndex = 0;

			while ((line = reader.readLine()) != null) {
				String[] values = line.split(","); // Split by comma (modify if delimiter is different)

				if (rowIndex == 0 && hasHeaders) {
					this.columnLabels.addAll(Arrays.asList(values));
					if (!columnLabels.contains("index")) {
						columnLabels.add(0, "index");
					}
					continue; // Skip the header row
				}

				Map<String, String> rowData = new HashMap<>();
				if (!hasHeaders) {
					for (int i = 0; i < values.length; i++) {
						columnLabels.add("Column" + (i + 1));
					}
					if (!columnLabels.contains("index")) {
						columnLabels.add(0, "index");
					}
					hasHeaders = true; // Set headers as initialized after first row
				}

				rowData.put("index", String.valueOf(rowIndex));
				for (int i = 0; i < values.length; i++) {
					if (i >= columnLabels.size() - 1)
						break;
					rowData.put(columnLabels.get(i + 1), values[i].trim());
				}
				this.data.add(rowData);
				rowIndex++;
			}
		}
	}

	/**
	 * Applies regular expression transformations to specific columns in the
	 * Dataframe. This method processes columns in the dataset and applies the given
	 * regex, replacing the column value with the first group captured.
	 *
	 * @param columnHeaderToRegexMap A mapping of column names to their respective
	 *                               regex patterns.
	 */
	public Map<String, Integer> preProcessRegex(Map<String, String> columnHeaderToRegexMap) {
		Map<String, Integer> captureCounter = new HashMap<String, Integer>();
		// Iterate over the specified columns in the map
		for (Map.Entry<String, String> entry : columnHeaderToRegexMap.entrySet()) {
			String column = entry.getKey(); // The column name
			String regex = entry.getValue(); // The regex pattern for this column

			// Ensure the column exists in the Dataframe
			if (!columnLabels.contains(column)) {
				System.err.println("Warning: Column '" + column + "' does not exist in the Dataframe. Skipping.");
				continue;
			}

			// Compile the regex pattern
			Pattern pattern = Pattern.compile(regex);

			int matchingEntries = 0;

			// Iterate over each row to apply the regex
			for (Map<String, String> row : data) {
				String originalValue = row.get(column);
				if (originalValue == null || originalValue.isEmpty()) {
					continue; // Skip empty or null values
				}

				// Match the regex on the current value
				Matcher matcher = pattern.matcher(originalValue);
				if (matcher.find() && matcher.groupCount() >= 1) {
					// Replace with the first group captured
					row.put(column, matcher.group(1));
					matchingEntries++;
				}
			}

			// Place the count of entries that satisfied the Regex in the "Header->Count"
			// map.

			captureCounter.put(column, matchingEntries);
		}

		return captureCounter;
	}

	/**
	 * Returns a list of rows corresponding to the passed list of indices.
	 *
	 * @param indices List of zero-based indices of the rows to retrieve.
	 * @return A list of Map<String, String> representing the selected rows' data.
	 */
	public List<Map<String, String>> get(List<Integer> indices) {
		// Use a HashSet for constant time complexity look-up
		Set<Integer> indexSet = new HashSet<>(indices);

		// Stream over the data and collect rows where their index is in the indexSet
		List<Map<String, String>> selectedRows = IntStream.range(0, data.size()).filter(indexSet::contains)
				.mapToObj(data::get).collect(Collectors.toList());

		// Add a "1" to all entries under the key "matchedByID"

		selectedRows.forEach(row -> row.put("matchedByID", "1"));

		return selectedRows;
	}

	/**
	 * Extracts column labels from the header row.
	 *
	 * @param headerRow          The row containing the column labels.
	 * @param isFirstColumnIndex Whether the first column is the index column.
	 * @return A list of column labels.
	 */
	private List<String> extractColumnLabels(Row headerRow, boolean isFirstColumnIndex) {
		List<String> labels = new ArrayList<>();
		if (headerRow == null) {
			throw new IllegalArgumentException("Header row is missing.");
		}
		for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
			String label = headerRow.getCell(colIndex).getStringCellValue().trim();
			if (colIndex > 0 || !isFirstColumnIndex) { // Skip the first column if it's the index
				labels.add(label);
			}
		}
		return labels;
	}

	/**
	 * Extracts data from a row in the sheet and adds the "index" column.
	 *
	 * @param row                The row to extract data from.
	 * @param rowIndex           The index of the row (if generated automatically).
	 * @param isFirstColumnIndex Whether the first column is the index column.
	 * @return A map representing the row's data: columnName -> cellValue.
	 */
	private Map<String, String> extractRowData(Row row, int rowIndex, boolean isFirstColumnIndex) {
		Map<String, String> rowData = new HashMap<>();
		DataFormatter formatter = new DataFormatter();

		// If the first column is NOT the index, auto-generate an "index" value
		if (!isFirstColumnIndex) {
			rowData.put("index", String.valueOf(rowIndex));
		}

		// Define the offset. If you inserted "index" at columnLabels[0],
		// the real header columns start at index 1.
		int labelOffset = isFirstColumnIndex ? 0 : 1;

		// Iterate up to however many columns the Excel row actually has
		int lastCellNum = row.getLastCellNum(); // # of actual cells in Excel
		for (int colIndex = 0; colIndex < lastCellNum; colIndex++) {

			// Make sure we don't go out of range in columnLabels
			if (colIndex + labelOffset >= columnLabels.size()) {
				break;
			}

			String columnName = columnLabels.get(colIndex + labelOffset);
			Cell cell = row.getCell(colIndex);
			String cellValue = (cell != null) ? formatter.formatCellValue(cell) : "";
			rowData.put(columnName, cellValue);
		}

		return rowData;
	}

	/**
	 * Displays the data stored in the Dataframe in a tabular format (console).
	 */
	public void display() {
		System.out.println(String.join(" | ", columnLabels));
		System.out.println("-------------------------------------------"); // Separator

		for (Map<String, String> row : data) {
			for (String label : columnLabels) {
				System.out.print(row.getOrDefault(label, "N/A") + " | ");
			}
			System.out.println();
		}
	}

	/**
	 * Looks up a row based on a key-value pair.
	 *
	 * @param column The column to search.
	 * @param value  The value to match.
	 * @return The first matching row, or null if not found.
	 */
	public Map<String, String> lookup(String column, String value) {
		for (Map<String, String> row : data) {
			if (value.equals(row.get(column))) {
				return row;
			}
		}
		return null;
	}

	/**
	 * Returns the i-th row in the Dataframe.
	 *
	 * @param i zero-based index of the row.
	 * @return A Map<String, String> representing that row's data.
	 */
	public Map<String, String> get(int i) {
		return data.get(i);
	}

	/**
	 * Returns the total number of rows in this Dataframe.
	 */
	public int size() {
		return data.size();
	}

	/**
	 * Allows iteration over rows in the Dataframe using a for-each loop.
	 *
	 * @return Iterator over Map<String, String>.
	 */
	@Override
	public Iterator<Map<String, String>> iterator() {
		return data.iterator();
	}

	// -------------------------------------------------------------------------
	// Main method for demonstration
	// -------------------------------------------------------------------------
	public static void main(String[] args) {
		try {
			Dataframe dataframe = new Dataframe();
			String filePath = "example.xlsx"; // Path to your Excel file
			String sheetName = "Sheet1"; // Name of the sheet

			dataframe.loadFromExcel(filePath, sheetName, false); // false = index is auto-generated

			// Display the data
			dataframe.display();

			// Example lookup
			Map<String, String> result = dataframe.lookup("Name", "John Doe");
			System.out.println("Lookup Result: " + result);

			// Demonstrating index-based access
			System.out.println("Row at index 0: " + dataframe.get(0));

			// Demonstrating iteration (for-each)
			for (Map<String, String> row : dataframe) {
				System.out.println("Iterated row: " + row);
			}

			// Demonstrating size
			System.out.println("Total rows: " + dataframe.size());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}