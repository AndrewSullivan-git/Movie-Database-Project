/* Name: Andrew Sullivan
 * N#: N01504763
 * COP 3703
 * Movie Database Final Project
 * Date: 8/2/2024 
 * */

import java.sql.*;
import java.io.*;

public class movieDatabase {
    static final String DB_URL = "jdbc:mysql://localhost:3306/?useSSL=false";
    static final String USER = "root";
    static final String PASS = "Sullivan";

    //main method establishes jdbc connection, creates the database, and creates the necessary tables
    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE IF EXISTS Final_Project");
                stmt.executeUpdate("CREATE DATABASE Final_Project");
                stmt.executeUpdate("USE Final_Project");

                createMoviesTable(stmt, conn);
                createTable(stmt, conn, "languages", "spoken_languages.csv", 3);
                createTable(stmt, conn, "cast", "cast.csv", 6);
                createTable(stmt, conn, "countries", "countries.csv", 3);
                createTable(stmt, conn, "crew", "crew.csv", 6);
                createTable(stmt, conn, "genres", "genres.csv", 3);
                createTable(stmt, conn, "keywords", "keywords.csv", 3);
                createTable(stmt, conn, "production", "production.csv", 3);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //method to create first table for movies
    private static void createMoviesTable(Statement stmt, Connection conn) throws SQLException, IOException {
        String createMoviesTable = "CREATE TABLE IF NOT EXISTS movies ("
                + "movie_id INT NOT NULL, "
                + "movie_title VARCHAR(1000) NULL, "  
                + "movie_release_date VARCHAR(20) NULL, "
                + "movie_language VARCHAR(10) NULL, "
                + "movie_popularity FLOAT NULL, "
                + "movie_revenue FLOAT NULL, "
                + "movie_budget FLOAT NULL, "
                + "movie_status VARCHAR(1000) NULL, "  
                + "movie_tagline VARCHAR(1000) NULL, "
                + "movie_vote_average FLOAT NULL, "
                + "movie_vote_count FLOAT NULL, "
                + "movie_runtime FLOAT NULL, "
                + "PRIMARY KEY (movie_id), "
                + "UNIQUE INDEX movie_id_UNIQUE (movie_id ASC))";
        stmt.executeUpdate(createMoviesTable);

        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\drew6\\Desktop\\Final Project CSV's\\movie.csv"));
             PreparedStatement pstmt = conn.prepareStatement("INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] token = line.split(",");
                pstmt.setInt(1, Integer.parseInt(token[0]));
                pstmt.setString(2, token[1]);
                pstmt.setString(3, token[2]);
                pstmt.setString(4, token[3]);
                pstmt.setFloat(5, Float.parseFloat(token[4]));
                pstmt.setFloat(6, Float.parseFloat(token[5]));
                pstmt.setFloat(7, Float.parseFloat(token[6]));
                pstmt.setString(8, token[7]);
                pstmt.setString(9, token[8]);
                pstmt.setFloat(10, Float.parseFloat(token[9]));
                pstmt.setFloat(11, Float.parseFloat(token[10]));
                pstmt.setFloat(12, Float.parseFloat(token[11]));
                pstmt.executeUpdate();
            }
            System.out.println("Movies Table Created");
        }
    }
    
    //method to create the rest of the tables
    private static void createTable(Statement stmt, Connection conn, String tableName, String csvFile, int columnCount) throws SQLException, IOException {
        String createTable = getCreateTable(tableName);
        stmt.executeUpdate(createTable);

        try (BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\drew6\\Desktop\\Final Project CSV's\\" + csvFile));
             PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (" + getPlaceholders(columnCount) + ")")) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] token = line.split(",");
                for (int i = 0; i < columnCount; i++) {
                    pstmt.setString(i + 1, token[i]);
                }
                pstmt.executeUpdate();
            }
            System.out.println(tableName + " Table Created");
        }
    }
    
    //method sets parameters for the relevant tables 
    private static String getCreateTable(String tableName) {
        switch (tableName) {
            case "languages":
                return "CREATE TABLE IF NOT EXISTS spoken_languages ("
                        + "movie_id INT NOT NULL, "
                        + "spoken_languages_id VARCHAR(1000) NULL, "  
                        + "spoken_language VARCHAR(1000) NULL, "  
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "cast":
                return "CREATE TABLE IF NOT EXISTS cast ("
                        + "movie_id INT NOT NULL, "
                        + "actor_id INT NOT NULL, "
                        + "actor VARCHAR(1000) NULL, "  
                        + "gender INT DEFAULT NULL, "
                        + "role_id INT NOT NULL, "
                        + "role VARCHAR(1000) NULL, "  
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "countries":
                return "CREATE TABLE IF NOT EXISTS countries ("
                        + "movie_id INT NOT NULL, "
                        + "country_id VARCHAR(1000) NULL, " 
                        + "country VARCHAR(1000) NULL, "  
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "crew":
                return "CREATE TABLE IF NOT EXISTS crew ("
                        + "movie_id INT NOT NULL, "
                        + "crewMember_id INT NOT NULL, "
                        + "crewMember VARCHAR(1000) NULL, "  
                        + "gender INT NOT NULL, "
                        + "job VARCHAR(1000) NULL, "  
                        + "job_id VARCHAR(1000) NULL, "  
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "genres":
                return "CREATE TABLE IF NOT EXISTS genres ("
                        + "movie_id INT NOT NULL, "
                        + "genre_id INT NOT NULL, "
                        + "genre VARCHAR(1000) NULL, "  
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "keywords":
                return "CREATE TABLE IF NOT EXISTS keywords ("
                        + "movie_id INT NOT NULL, "
                        + "keyword_id INT NOT NULL, "
                        + "keyword VARCHAR(1000) NULL, "
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            case "production":
                return "CREATE TABLE IF NOT EXISTS production ("
                        + "movie_id INT NOT NULL, "
                        + "company_id VARCHAR(1000) NULL, "
                        + "company VARCHAR(1000) NULL, "
                        + "FOREIGN KEY (movie_id) REFERENCES movies(movie_id))";
            default:
                throw new IllegalArgumentException("Unknown table: " + tableName);
        }
    }
    //method creates the placeholder values for table creation
    private static String getPlaceholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
