package com.nsk.spring.Springpro;





import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class  App {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/miii";  // Update with your DB
        String username = "root";  // Update with your username
        String password = "root";  // Update with your password
        
        String csvFilePath = "C:/Users/shyma/OneDrive/Desktop/book8.csv";  // Update with the CSV file path
        String tableName = "csv_import_table"; // You can make this dynamic based on the file

        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {
            connection.setAutoCommit(false);  // Enable transaction
            
            // Step 1: Read CSV file
            CSVReader csvReader = new CSVReader(new FileReader(csvFilePath));
            String[] headers = csvReader.readNext(); // Read the first line (headers)
            
            // Step 2: Automatically create table based on CSV headers
            createTable(connection, tableName, headers);
            
            // Step 3: Insert CSV data into the table
            insertData(connection, csvReader, tableName, headers);
            
            connection.commit();
            System.out.println("Data inserted successfully.");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Step 2: Create table dynamically based on CSV headers
    private static void createTable(Connection connection, String tableName, String[] headers) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

        for (String header : headers) {
            createTableSQL.append(header).append(" VARCHAR(255), ");  // Assuming all data types as VARCHAR for simplicity
        }
        createTableSQL.setLength(createTableSQL.length() - 2);  // Remove last comma
        createTableSQL.append(")");

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL.toString());
        }
    }

    // Step 3: Insert CSV data into the table
    private static void insertData(Connection connection, CSVReader csvReader, String tableName, String[] headers) throws SQLException {
        StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (");
        
        for (String header : headers) {
            insertSQL.append(header).append(", ");
        }
        insertSQL.setLength(insertSQL.length() - 2);  // Remove last comma
        insertSQL.append(") VALUES (");
        
        for (int i = 0; i < headers.length; i++) {
            insertSQL.append("?, ");
        }
        insertSQL.setLength(insertSQL.length() - 2);  // Remove last comma
        insertSQL.append(")");
        
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL.toString())) {
            String[] rowData;
            try {
				while ((rowData = csvReader.readNext()) != null) {
				    for (int i = 0; i < rowData.length; i++) {
				        preparedStatement.setString(i + 1, rowData[i]);  // Set data dynamically
				    }
				    preparedStatement.addBatch();  // Add to batch for faster inserts
				}
			} catch (CsvValidationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            preparedStatement.executeBatch();  // Execute batch insert
        }
    }
}