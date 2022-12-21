package com.study.database.util;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDDL {
    public static Connection connection=HbaseConnection.connection;
    public static void createNamespace(String namespace) throws IOException {//创建命名空间
        Admin admin=connection.getAdmin();
        NamespaceDescriptor.Builder builder=NamespaceDescriptor.create(namespace);
        builder.addConfiguration("user","user");
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
        }

        admin.close();
    }

    public static boolean isTableExists(String namespace,String tableName) throws IOException {//判断表格是否存在
        Admin admin=connection.getAdmin();//获取admin

        boolean exists= false;//判断表格存在
        try {
            exists = admin.tableExists(TableName.valueOf(namespace,tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();//关闭admin
        return exists;

    }
    public static void createTable(String namespace,String tableName, String... columnFamilies) throws IOException {//创建表
        if(columnFamilies.length==0){
            System.out.println("列族数不能为零");
            return;
        }
        Admin admin=connection.getAdmin();//获取admin

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        for (String columnFamily : columnFamilies) {
            //创建列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("表格存在");
            e.printStackTrace();
        }

        admin.close();


    }

}
