package org.test;

import com.datastax.driver.core.*;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

public class test {

    private static final Logger logger = Logger.getLogger(test.class);
    private static Session caSession;
    private static Cluster caCluster;

    public static void main(String[] args) throws Exception {
      // Argument list: node_id [file]
      logger.info("Starting benchmark for Cassandra and HBase");

      String table_name = "test_casssandra";

      String num = args[0];

      // Using every node in the cluster as a potential contact point to avoid issues where cassandra is not running on a single node
      caCluster = Cluster.builder().addContactPoints("compute-0-0.local", "compute-0-1.local", "compute-0-2.local", "compute-0-3.local", "compute-0-4.local", "compute-0-5.local", "compute-0-6.local", "compute-0-7.local", "compute-0-8.local").build();
      caSession = caCluster.connect();

      for(String arg : args) {
        logger.info(arg);
      }

      if(args.length == 1) {
        logger.info("Retrieving data");
        long startTime = System.nanoTime();
        get_cassandra(num, table_name);
        long endTime = System.nanoTime();
        logger.info("Time taken to get from Cassandra: " + ((endTime - startTime) / 1000000) + " milliseconds");
      } else {
        logger.info("Adding data");
        String target = args[1];

        // Setup Hbase and cassandra
        setup_cassandra(table_name);
      
        BufferedReader file = new BufferedReader(new FileReader(target));     

        // Add Hbase and cassandra (time it)
        long startTime = System.nanoTime();
        add_cassandra(num, file, table_name);
        long endTime = System.nanoTime();
        logger.info("Time taken to add to Cassandra: " + ((endTime - startTime) / 1000000) + " milliseconds");
      }
      caSession.close();
      System.exit(1);
    }

    private static Map<String, String> get_entry(BufferedReader file) throws Exception {
      Map<String, String> map = new HashMap<String,String>();
      while(file.ready()) {
        String line = file.readLine();
        if(line.startsWith("//")) {
          return map;
        } else {
          if(line.startsWith("  ")) {
            map.put("SEQ", line.substring(2));
          } else {
            map.put(line.substring(0,2), line.substring(2));
          }
        }
      }
      return null;
    }

    private static String[] parse_entry(Map<String, String> entry) {
      String[] matchArray = {"ID", "AC", "DT", "GN", "OS", "RN", "RP", "RC", "RX", "RA", "RT", "RL", "CC", "DR", "PE", "KW", "FT", "SQ", "SEQ"};
      String[] retArray = new String[matchArray.length];
      int i = 0;
      for(String key : matchArray) {
        if(entry.get(key) != null) {
          retArray[i] = entry.get(key);
        } else {
          retArray[i] = "";
        }
        i++;
      }
      return retArray; 
    }

    private static int get_cassandra(String node, String table_name) {
      ResultSet res = caSession.execute("SELECT * FROM test.testy where sort = \'" + node + "\';");
      Iterator<Row> it = res.iterator();
      while(it.hasNext()) {
        Row result = it.next();
        int i = 0;
        while(i < 20) {
          String data = result.getString(i);
          i++;
        }
      }
      return 1;
    }

    private static int add_cassandra(String node, BufferedReader file, String table_name) throws Exception {
      PreparedStatement stat = caSession.prepare("INSERT INTO testy " +
                                                 "(SORT, ID, AC, DT, DE, GN, OS, RN, RP, RC, RX, RA, RT, RL, CC, DR, PE, KW, FT, SQ, SEQ) " +
                                                 "VALUES (? ,?,?,?,?, ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?);");
      Map<String,String> entry = get_entry(file);
      while(entry != null) {

        BoundStatement bs = stat.bind();
        String[] entry_res = parse_entry(entry);
        bs.setString(0, node);
        int i = 1;
        for(String field : entry_res) {
          bs.setString(i, field);
          i++;
        }
        caSession.execute(bs);
        entry = get_entry(file);
      }
      
      return 1;
    }

    private static int setup_cassandra(String table_name) {
      logger.info("Setting up Cassandra table for uniprot data");
      caSession.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication " + 
                        "= {'class':'SimpleStrategy', 'replication_factor':3};");
      caSession.execute("USE test;");
      try {
        //caSession.execute("DROP TABLE testy;");
        caSession.execute("CREATE TABLE IF NOT EXISTS testy (" + 
                        "SORT text, " +
                        "ID text, " +
                        "AC text, " +
                        "DT text, " +
                        "DE text, " +
                        "GN text, " +
                        "OS text, " +
                        "RN text, " +
                        "RP text, " +
                        "RC text, " +
                        "RX text, " +
                        "RA text, " +
                        "RT text, " +
                        "RL text, " +
                        "CC text, " +
                        "DR text, " +
                        "PE text, " +
                        "KW text, " +
                        "FT text, " +
                        "SQ text, " +
                        "SEQ text, " +
                        "PRIMARY KEY (SORT, ID) " +
                        ");");
      } catch(Exception E) {
        logger.warn("Table already exists, ignoring...");
      }
      return 1;
    }
}
