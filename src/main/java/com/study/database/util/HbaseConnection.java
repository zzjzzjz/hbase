package com.study.database.util;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnection {
    public static Connection connection=null;
    static {

        try {
            connection= ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeConnection() throws IOException {
        if (connection!=null){
            connection.close();
        }
    }
    public static void main(String args[]){
        System.out.println(HbaseConnection.connection);
        try {
            HbaseConnection.closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
