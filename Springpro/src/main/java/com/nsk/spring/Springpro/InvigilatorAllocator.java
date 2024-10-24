package com.nsk.spring.Springpro;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class InvigilatorAllocator {

    // Database connection details
    private static final String DB_URL = "jdbc:mysql://localhost:3306/faculty_db"; // Update with your DB name
    private static final String USER = "root"; // Update with your DB username
    private static final String PASSWORD = "root"; // Update with your DB password

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            // Assume inputStream is obtained from a file or other source
            InputStream inputStream = System.in; // Change as necessary for actual input stream

            processCsv(inputStream, connection);
        } catch (SQLException e) {
            System.out.println("Database connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void processCsv(InputStream inputStream, Connection connection) {
        List<String[]> dataList = new ArrayList<>();

        String[] dateArray;
        String[] slotArray;
        int[] numberArray;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            System.out.println("Reading CSV file...");

            String line;
            br.readLine(); // Skip the header

            List<String> dates = new ArrayList<>();
            List<String> slots = new ArrayList<>();
            List<Integer> numbers = new ArrayList<>();

            // Read CSV data
            while ((line = br.readLine()) != null) {
                System.out.println("Processing line: " + line);
                String[] values = line.split(","); // Split by comma (CSV format)
                dataList.add(values);

                dates.add(values[0]);
                slots.add(values[1]);
                numbers.add(Integer.parseInt(values[2]));
            }

            dateArray = dates.toArray(new String[0]);
            slotArray = slots.toArray(new String[0]);
            numberArray = numbers.stream().mapToInt(Integer::intValue).toArray();

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");

            System.out.printf("\ndates            slots   number\n");

            for (int i = 0; i < dateArray.length; i++) {
                String date = dateArray[i];
                String slot = slotArray[i];
                int required = numberArray[i];

                System.out.printf("%s     %s        %d\n", date, slot, required);

                // Determine the column name based on the slot
                String column = slot.toUpperCase() + (slot.endsWith("M") ? "" : "A");
                System.out.println("Looking for column: " + column);

                try {
                    // Query for available faculty based on the specific slot
                    String query = "SELECT id, name FROM faculty WHERE " + column + " = ?";
                    System.out.println("Executing query: " + query);

                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    preparedStatement.setString(1, "Yes"); // Assuming availability is marked with "Yes"
                    ResultSet resultSet = preparedStatement.executeQuery();

                    List<Faculty> availableFaculty = new ArrayList<>();
                    while (resultSet.next()) {
                        String id = resultSet.getString("id");
                        String name = resultSet.getString("name");

                        // Get the allocation count for each available faculty
                        int count = getAllocationCount(connection, id);
                        availableFaculty.add(new Faculty(id, name, count));
                    }

                    // Sort faculty by their allocation count (ascending)
                    availableFaculty.sort(Comparator.comparingInt(f -> f.allocationCount));

                    // Select the required number of faculty who are least allocated
                    List<String> assignedFaculty = new ArrayList<>();
                    for (int j = 0; j < Math.min(required, availableFaculty.size()); j++) {
                        Faculty selectedFaculty = availableFaculty.get(j);
                        assignedFaculty.add(selectedFaculty.name);
                        System.out.println("Assigning " + selectedFaculty.name + " (ID: " + selectedFaculty.id + ")");

                        // Update the allocation count in the database
                        updateAllocationCount(connection, selectedFaculty.id);
                    }

                    // Output the assigned faculty members
                    System.out.println("OUTPUT");
                    for (String facultyName : assignedFaculty) {
                        System.out.println(facultyName);
                    }

                } catch (SQLException e) {
                    System.out.println("Error during faculty selection for date: " + date + " and slot: " + slot);
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            System.out.println("Error occurred while processing CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static int getAllocationCount(Connection connection, String facultyId) {
        int count = 0;
        try {
            String query = "SELECT allocation_count FROM faculty WHERE id = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, facultyId);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                count = resultSet.getInt("allocation_count");
            }
        } catch (SQLException e) {
            System.out.println("Error fetching allocation count for faculty ID: " + facultyId);
            e.printStackTrace();
        }
        return count;
    }

    private static void updateAllocationCount(Connection connection, String facultyId) {
        try {
            String query = "UPDATE faculty SET allocation_count = allocation_count + 1 WHERE id = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, facultyId);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Error updating allocation count for faculty ID: " + facultyId);
            e.printStackTrace();
        }
    }

    private static class Faculty {
        String id;
        String name;
        int allocationCount;

        Faculty(String id, String name, int allocationCount) {
            this.id = id;
            this.name = name;
            this.allocationCount = allocationCount;
        }
    }
}
