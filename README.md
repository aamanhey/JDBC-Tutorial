# JDBC Tutorial
March 2022

## Introduction

This tutorial will show you how to load Businesses from the Yelp dataset into mySql using a JDBS driver and Java. This tutorial is on Mac and requires a MySql account.
In order to do this you will need to download some packages:
- [Java SQL package documentation](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html)
- [Yelp dataset](https://www.yelp.com/dataset)
- [JDBC driver here](https://dev.mysql.com/downloads/connector/j/)
- [JSON Parser](https://code.google.com/archive/p/json-simple/downloads)

For the Yelp dataset you can see the structure of the business objects, as well as the other objects, on the [documentation page](https://www.yelp.com/dataset/documentation/main). For the JDBS driver, this tutorial used the "platform independent" driver. Download your preferred file, unzip it, and place it in a known directory. The file's name should be similar to "mysql-connector-java-8.0.28" and inside there is going to be a jar file with a similar name, this is the java code for the driver.


## Setting Up IDE

Open up your IDE, for this tutorial I am using [IntelliJ](https://www.jetbrains.com/idea/).
Create a new Java project and a new Java file in your src directory called LoadData.
We need to add the driver to our Java environment to use it. This can be done by navigating to your project libraries:

    File > Project Structure > Project Settings > Libraries

Use "+" to add a new Java Project Library and you will be prompted to add the jar file, add the jar files for the JDBC driver and the simple-json parser.

## Setting Up Database

In your LoadData class, we need to add 4 class variables:

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/jdbc_example";
    static final String USER = "root";
    static final String PASSWORD = "<mySql password>";

The JDBC_DRIVER specifies what driver your going to use to connect your java code to mySql.
The DB_URL specifices the url of your locally running mySql server.
The last two are your login credentials to access your local mySql server.

To be able to connect to mySql the server needs to be running so navigate to your terminal and start your mySql server.

In our main method, and in most of our methods for this project, we want to place our code in a try-catch block so that if an error occurs we can get a message instead of the program crashing. The error placed in our catch will be an SQLException error. A connection needs to be established to the mySql server so we're going to call the DriverManager class and use the getConnection method. Your code should look something like this:

    public static void main(String[] args) {
  		Connection conn = null;
  		// open a connection
  		try{
  			System.out.println("Connecting to database ...");
  			conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
        System.out.println("Connection established ...");
  		}catch(SQLException ex){
  			ex.printStackTrace();
  		}
    }

Compiling the code should output:

    Connecting to database ...
    Connection established ...

Our mySql commands are sent as strings so we can write out our commands as we would in mySql and save them as strings.
To setup the database, create a createDatabase method with parameter conn, the connection we have established.
To execute sql commands from our program we're going to use Statements, which will take the string commands as parameters.
We're using the PreparedStatement class because it is more secure than just sending a string, which could be edited while in transmission.
We're then going to create a string to hold the database command.
The next portion of executing the command will be wrapped in a try-catch block and execute the statement with either the executeUpdate or executeQuery methods. The difference being that executeQuery returns a result set while executeUpdate does not. Since we're just creating the database we'll use executeUpdate. The createDatabase method should look like this now:

    public static void createDatabase(Connection conn){
      **PreparedStatement statement = null;**
      String createDB = "CREATE DATABASE YELP";
      try{
        **statement = conn.prepareStatement(createDB);**
        System.out.println(statement);
        **statement.executeUpdate();**
      }catch(SQLException e){
        e.printStackTrace();
      }
    }

Now calling this method should produce the following output in our console:

    Connecting to database ...
    Connection established ...
    com.mysql.cj.jdbc.ClientPreparedStatement: CREATE DATABASE YelpDemo

If you go to your mqSql server and use the "show databases" command you should now see a "YelpDemo" database.
Let's comment out the method call so we don't run into an error when running the code again since the database already exists.
Now creating the table for business is going to be the same as creating the database except the command string is going to be different.
Instead of taking all the field from each business, let's just take some that might be useful, such as business_id, name, review_count, stars, and category.
We also need to create a method to choose which database we want to put our table into, which will be the same as our pickDB method except for the string command we're going to be using.

    public static void pickDB(Connection conn){
      PreparedStatement statement = null;
      String useDatabase = **"USE YelpDemo";**
      try{
        statement = conn.prepareStatement(useDatabase);
        System.out.println(statement);
        statement.executeUpdate();
      }catch(SQLException e){
          e.printStackTrace();
      }
    }

Calling both of these methods, our console should show the "use YelpDemo" and create table mySql commands we used.

## Reading JSON Data

To import the data we're going to use a combination of the JSON parser from simple-json and BufferedReader, since the Yelp dataset has each of the business objects on their own line and they are not help in a single top-level array. To start the JSONParser and a set for each business object needs to be created.

    JSONParser parser = new JSONParser();
    HashSet<String[]> data = new HashSet<>();

Then it is just a matter of reading each line in, parsing is with the parse method and getting the desired fields, such as

    reader = new BufferedReader(new FileReader(filename));
    String line = reader.readLine();
    while (line != null) {
      System.out.println(line);
      **JSONObject business = (JSONObject) parser.parse(line);**
      line = reader.readLine();

      // business_id, name, review_count, stars, and category
      String business_id = (String) **business.get("business_id")**;
      System.out.println(business_id);

After each desired field is retrieved we can save them as a list and store that list in the set for our businesses.
Returning that set to our main, we can use that object to create each of our insert commands for mySql.

## Importing to mySql

To import data into mySql we need to create a method to iterate through our data and add each record as an insert. Our query is going to be

  String insertQuery = "INSERT INTO Business VALUES (?, ?, ?, ?, ?)";

The ? will be filled in by each of our records using the set<Type> method (e.g. setString(), setDouble()). The resulting method should look something like the following:

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

Using the "select * from Business;" command in the terminal, we should see all the records from our json file in mySql.
