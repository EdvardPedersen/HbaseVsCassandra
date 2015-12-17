package org.test;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.*;



import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

public class test extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(test.class);

    public static void main(String [] args) throws Exception {
      int result = ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new test(), args);
      System.exit(result);
    }

    public int run(String[] args) throws Exception {
      org.apache.hadoop.conf.Configuration argConf = HBaseConfiguration.create(getConf());
      logger.info("Starting benchmark for Cassandra and HBase");

      String table_name = "test_casssandra";

      String num = argConf.get("node", "0");

      if(argConf.get("add") == null) {
        logger.info("Retrieving data");

        long startTime = System.nanoTime();
        get_hbase(num, table_name, argConf);
        long endTime = System.nanoTime();
        logger.info("Time taken to get from HBase: " + ((endTime - startTime) / 1000000) + " milliseconds");
      } else {
        logger.info("Adding data");
        String target = argConf.get("file", "test.dat");

        // Setup Hbase and cassandra
        setup_hbase(table_name, argConf);
      
        BufferedReader file = new BufferedReader(new FileReader(target));     

        // Add Hbase and cassandra (time it)
        long startTime = System.nanoTime();
        add_hbase(num, file, table_name, argConf);
        long endTime = System.nanoTime();
        logger.info("Time taken to add to HBase: " + ((endTime - startTime) / 1000000) + " milliseconds");
      }
      return 1;
    }

    private static int add_hbase(String num, BufferedReader file, String table_name, org.apache.hadoop.conf.Configuration conf) throws Exception {
      logger.info("Adding file to table " + table_name + " in HBase");

      HBaseAdmin hbAdmin = new HBaseAdmin(conf);

      HTable ourTable = new HTable(conf, table_name);
      Put ourPut = null;
      int numAdded = 0;

      Map<String,String> map = get_entry(file);
      while(map != null) {
        ourPut = new Put((num + map.get("ID")).getBytes());
        for(Map.Entry<String,String> entry : map.entrySet()){
          ourPut.add("d".getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
        }
        ourTable.put(ourPut);
        map = get_entry(file);
      }

      return 1;
    }

    private static int setup_hbase(String table_name, org.apache.hadoop.conf.Configuration conf) {
      // Setup test table
      logger.info("Setting up HBase table for uniprotkb data");

      try {
        HBaseAdmin hbAdmin = new HBaseAdmin(conf);
        //if(hbAdmin.tableExists(table_name)) {
        //  hbAdmin.disableTable(table_name);
        //  hbAdmin.deleteTable(table_name);
        //}
        HTableDescriptor hTable = new HTableDescriptor(table_name);
        HColumnDescriptor hCD = new HColumnDescriptor("d".getBytes(), 2000, HColumnDescriptor.DEFAULT_COMPRESSION, false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER);
        hTable.addFamily(hCD);
        hbAdmin.createTable(hTable);
      } catch (Exception E) {
        logger.warn("Unable to create HBase table: \n" + E.toString());
      }
      return 1;
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

    private static int get_hbase(String num, String table_name, org.apache.hadoop.conf.Configuration conf) throws Exception {
      // Create scan from num to num+1
      // Retrieve the data
      HTable ourTable = new HTable(conf, table_name);
      Integer stop_row = Integer.valueOf(num) + 1;
      Scan ourScan = new Scan(num.getBytes(), stop_row.toString().getBytes());
      ResultScanner scan_res = ourTable.getScanner(ourScan);
      for(Result result = scan_res.next(); (result != null); result = scan_res.next()) {
        for(KeyValue res : result.list()) {
          //logger.info(res.getKeyString());
        }
      }

      return 1;
    }
}
