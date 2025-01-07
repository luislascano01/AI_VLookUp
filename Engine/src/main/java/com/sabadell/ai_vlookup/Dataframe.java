package com.sabadell.ai_vlookup;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;

import java.util.*;

public class Dataframe {
    private final List<Map<String, String>> data;
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
     * @param filePath    Path to the Excel file.
     * @param sheetName   Name of the sheet to load.
     * @throws IOException If there is an issue reading the file.
     */
    @SuppressWarnings("resource")
	public void loadFromExcel(String filePath, String sheetName) throws IOException {
        // Open the Excel file
        FileInputStream fileInputStream = new FileInputStream(filePath);
        Workbook workbook = new XSSFWorkbook(fileInputStream);
        Sheet sheet = workbook.getSheet(sheetName);

        if (sheet == null) {
            throw new IllegalArgumentException("Sheet " + sheetName + " does not exist.");
        }

        // Extract column labels from the header row
        this.columnLabels = extractColumnLabels(sheet.getRow(0));

        // Load the data rows
        this.data.clear(); // Clear any existing data
        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) continue; // Skip empty rows
            Map<String, String> rowData = extractRowData(row);
            this.data.add(rowData);
        }

        workbook.close();
        fileInputStream.close();
    }

    /**
     * Extracts column labels from the header row.
     *
     * @param headerRow The row containing the column labels.
     * @return A list of column labels.
     */
    private List<String> extractColumnLabels(Row headerRow) {
        List<String> labels = new ArrayList<>();
        if (headerRow == null) {
            throw new IllegalArgumentException("Header row is missing.");
        }

        for (Cell cell : headerRow) {
            labels.add(cell.getStringCellValue().trim());
        }
        return labels;
    }

    /**
     * Extracts data from a row in the sheet.
     *
     * @param row The row to extract data from.
     * @return A map representing the row's data.
     */
    private Map<String, String> extractRowData(Row row) {
        Map<String, String> rowData = new HashMap<>();
        DataFormatter formatter = new DataFormatter();

        for (int colIndex = 0; colIndex < columnLabels.size(); colIndex++) {
            Cell cell = row.getCell(colIndex);
            String cellValue = cell != null ? formatter.formatCellValue(cell) : "";
            rowData.put(columnLabels.get(colIndex), cellValue);
        }

        return rowData;
    }

    /**
     * Displays the data stored in the Dataframe in a tabular format.
     */
    public void display() {
        System.out.println(String.join(" | ", columnLabels)); // Print column labels
        System.out.println("-".repeat(columnLabels.size() * 10)); // Separator

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

    // Main method for demonstration
    public static void main(String[] args) {
        try {
            Dataframe dataframe = new Dataframe();
            String filePath = "example.xlsx"; // Path to your Excel file
            String sheetName = "Sheet1"; // Name of the sheet

            dataframe.loadFromExcel(filePath, sheetName);

            // Display the data
            dataframe.display();

            // Example lookup
            Map<String, String> result = dataframe.lookup("Name", "John Doe");
            System.out.println("Lookup Result: " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
