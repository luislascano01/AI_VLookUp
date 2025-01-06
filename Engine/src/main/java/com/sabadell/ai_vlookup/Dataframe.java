package com.sabadell.ai_vlookup;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class Dataframe {
    private final List<Map<String, String>> data;

    /**
     * Constructor initializes an empty data frame.
     */
    public Dataframe() {
        this.data = new ArrayList<>();
    }

    /**
     * Loads data from an Excel sheet into the Data frame.
     *
     * @param filePath    Path to the Excel file.
     * @param sheetName   Name of the sheet to load.
     * @param columnLabels List of column labels to include.
     * @throws IOException If there is an issue reading the file.
     */	
    public void loadFromExcel(String filePath, String sheetName, List<String> columnLabels) throws IOException {
        // Load the Excel file
        FileInputStream fileInputStream = new FileInputStream(filePath);
        @SuppressWarnings("resource")
		Workbook workbook = new XSSFWorkbook(fileInputStream);
        Sheet sheet = workbook.getSheet(sheetName);

        if (sheet == null) {
            throw new IllegalArgumentException("Sheet with name " + sheetName + " does not exist.");
        }

        // Map column indices to the specified column labels
        Map<Integer, String> columnIndexToLabel = new HashMap<>();
        Row headerRow = sheet.getRow(0);

        if (headerRow == null) {
            throw new IllegalArgumentException("The sheet does not have a header row.");
        }

        for (Cell cell : headerRow) {
            String cellValue = cell.getStringCellValue().trim();
            if (columnLabels.contains(cellValue)) {
                columnIndexToLabel.put(cell.getColumnIndex(), cellValue);
            }
        }

        // Validate that all specified columns are present
        for (String label : columnLabels) {
            if (!columnIndexToLabel.containsValue(label)) {
                throw new IllegalArgumentException("Column " + label + " does not exist in the sheet.");
            }
        }

        // Load rows into the data structure
        this.data.clear(); // Clear any existing data
        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) continue; // Skip empty rows

            Map<String, String> rowData = new HashMap<>();
            for (Map.Entry<Integer, String> entry : columnIndexToLabel.entrySet()) {
                Cell cell = row.getCell(entry.getKey());
                String cellValue = cell != null ? cell.toString().trim() : ""; // Handle nulls gracefully
                rowData.put(entry.getValue(), cellValue);
            }
            this.data.add(rowData);
        }

        // Close resources
        workbook.close();
        fileInputStream.close();
    }

    /**
     * Returns the data stored in the Dataframe.
     *
     * @return List of Maps representing the rows of the dataframe.
     */
    public List<Map<String, String>> getData() {
        return data;
    }

    /**
     * Displays the data stored in the Dataframe in a tabular format.
     */
    public void display() {
        for (Map<String, String> row : data) {
            System.out.println(row);
        }
    }

    // Main method for demonstration
    public static void main(String[] args) {
        try {
            Dataframe dataframe = new Dataframe();
            String filePath = "example.xlsx"; // Path to your Excel file
            String sheetName = "Sheet1"; // Name of the sheet
            List<String> columnLabels = Arrays.asList("Name", "Age", "City"); // Columns to load

            dataframe.loadFromExcel(filePath, sheetName, columnLabels);

            // Display the data
            dataframe.display();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
