package com.yourname.minisql.region;

import com.yourname.minisql.region.manager.DatabaseManager;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("=== MiniSQL Region Server ===");
        System.out.println("Starting...");
        
        try (DatabaseManager db = new DatabaseManager("./data")) {
            System.out.println("Database started. Data directory: ./data");
            System.out.println("\nAvailable commands:");
            System.out.println("  CREATE TABLE users");
            System.out.println("  INSERT INTO users (id, name, age) VALUES ('1', 'Alice', '25')");
            System.out.println("  SELECT * FROM users WHERE id = '1'");
            System.out.println("  DELETE FROM users WHERE id = '1'");
            System.out.println("  stats - show engine statistics");
            System.out.println("  exit  - quit");
            System.out.println();
            
            Scanner scanner = new Scanner(System.in);
            
            while (true) {
                System.out.print("minisql> ");
                String line = scanner.nextLine().trim();
                
                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    System.out.println("Goodbye!");
                    break;
                }
                
                if (line.equalsIgnoreCase("stats")) {
                    db.printStats();
                    continue;
                }
                
                if (line.isEmpty()) {
                    continue;
                }
                
                String result = db.execute(line);
                System.out.println(result);
            }
            
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}