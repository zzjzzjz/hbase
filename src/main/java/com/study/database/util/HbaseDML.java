package com.study.database.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDML {
    public static Connection connection=HbaseConnection.connection;

    public static void putStringCell(String namespace,String tableName,String rowKey,String columnFamily,String columnName,String value) throws IOException {//插入数据
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));//获取table


        //插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();//关闭table
    }
    public static void putIntCell(String namespace,String tableName,String rowKey,String columnFamily,String columnName,int value) throws IOException {//插入数据
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));//获取table


        //插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();//关闭table
    }
}
