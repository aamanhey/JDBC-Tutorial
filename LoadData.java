import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

public class LoadData {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/jdbc_example";

    static final String USER = "root";
    static final String PASSWORD = "<mySql password>";

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

    public static void deleteDatabase(Connection conn){
        PreparedStatement statement = null;
        String createTable = "DROP DATABASE YelpDemo;";
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

    public static void insertBusinesses(Connection conn, HashSet<String[]> data){
        PreparedStatement statement = null;
        String insertQuery = "INSERT INTO Business VALUES (?, ?, ?, ?, ?)";
        try{
            // business_id, name, review_count, stars, and category
            Iterator<String[]> it = data.iterator();
            while (it.hasNext()) {
                String[] fields = it.next();
                statement = conn.prepareStatement(insertQuery);
                statement.setString(1, fields[0]);
                statement.setString(2, fields[1]);
                statement.setLong(3, Long.parseLong(fields[2]));
                statement.setDouble(4, Double.parseDouble(fields[3]));
                statement.setString(5, fields[4]);
                System.out.println(statement);
                statement.executeUpdate();
            }
        }catch (SQLException ex){
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection conn = null;

        // open a connection
        try{
            System.out.println("Connecting to database ...");
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            System.out.println("Connection established ...");

            // Create Database
            createDatabase(conn);

            // Choose Database
            pickDB(conn);

            // Get JSON Data
            HashSet<String[]> data = readJSON("<file_path>");

            // Import Data to mySql
            insertBusinesses(conn, data);

            // Create Businesses Table
            createBusinessTable(conn);

            // Delete Database
            deleteDatabase(conn);

        }catch(SQLException ex){
            ex.printStackTrace();
        }
    }
}
