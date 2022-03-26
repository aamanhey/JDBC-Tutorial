import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

public class LoadData {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/jdbc_example";

    static final String USER = "root";
    static final String PASSWORD = "hr6j!umC@AyR";

//    public static void insertMenu(Connection conn, String filename) {
//        PreparedStatement statement = null;
//        String insertQuery = "INSERT INTO Menu VALUES (?, ?, ?)";
//        try{
//            statement = conn.prepareStatement(insertQuery);
//            // setXXX() methods to set the values of these ?
//            statement.setString(1, "Coffee");
//            statement.setString(2, "Coffee");
//            statement.setDouble(3, 5);
//            System.out.println(statement);
//            // to execute the statement,
//            // executeQuery() -> return a ResultSet
//            // executeUpdate() -> to update the database without returning a ResultSet
//            // instead, it returns an integer that tells us how many records in the
//            // database were affected
//            statement.executeUpdate();
//        }catch (SQLException ex){
//            ex.printStackTrace();
//        }

//		// Reading from the csv file
//		List<List<String>> menuRecords = new ArrayList<>();
//		try (Scanner scanner = new Scanner(new File("menu.csv"));) {
//		    while (scanner.hasNextLine()) {
//		    	menuRecords.add(getRecordFromLine(scanner.nextLine()));
//		    }
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//    }

//    public static void insertOrders(Connection conn, String filename) {
//
//        // Reading from the csv file
//
//        List<List<String>> orderRecords = new ArrayList<>();
//        try (Scanner scanner = new Scanner(new File("order.csv"));) {
//            while (scanner.hasNextLine()) {
//                orderRecords.add(getRecordFromLine(scanner.nextLine()));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void createDatabase(Connection conn){
        PreparedStatement statement = null;
        String createDB = "CREATE DATABASE YelpDemo";
        try{
            statement = conn.prepareStatement(createDB);
            System.out.println(statement);
            statement.executeUpdate();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public static void pickDB(Connection conn){
        PreparedStatement statement = null;
        String useDatabase = "USE YelpDemo";
        try{
            statement = conn.prepareStatement(useDatabase);
            System.out.println(statement);
            statement.executeUpdate();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public static void createBusinessTable(Connection conn){
        PreparedStatement statement = null;
        String createTable = "CREATE TABLE Business (\n" +
                "  business_id varchar(22) not null,\n" +
                "  name varchar(100) not null,\n" +
                "  review_count smallint,\n" +
                "  stars decimal (2,1),\n" +
                "  category varchar(1000),\n" +
                "  Primary key(business_id))";
        try{
            statement = conn.prepareStatement(createTable);
            System.out.println(statement);
            statement.executeUpdate();
        }catch(SQLException e){
            e.printStackTrace();
        }
    }

    public static HashSet<String[]> readJSON(String filename){
        JSONParser parser = new JSONParser();
        HashSet<String[]> data = new HashSet<>();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filename));
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                JSONObject business = (JSONObject) parser.parse(line);
                line = reader.readLine();

                // business_id, name, review_count, stars, and category
                String business_id = (String) business.get("business_id");
                System.out.println(business_id);

                String name = (String) business.get("name");
                System.out.println(name);

                String review_count = Long.toString((Long) business.get("review_count"));
                System.out.println(review_count);

                String stars =  Double.toString((Double) business.get("stars"));
                System.out.println(stars);

                String category = (String) business.get("categories");
                System.out.println(category);

                String[] fields = new String[]{business_id, name, review_count, stars, category};

                data.add(fields);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement statement = null;

        // open a connection
        // execute a query -> constructed with String concatenation
        try{
            System.out.println("Connecting to database ...");
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            System.out.println("Connection established ...");

            // Create Database
//            createDatabase(conn);

            // Choose Database
            pickDB(conn);

            // Get JSON Data
            HashSet<String[]> data = readJSON("/Users/macintosh/IdeaProjects/JDBCYelpTutorial/src/business_struct.json");

            // Create Businesses Table
//            createBusinessTable(conn);
        }catch(SQLException ex){
            ex.printStackTrace();
        }
    }
}
