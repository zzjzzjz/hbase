package com.study.database.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class store {

    public static void createNamespaceAndTable(){//创建命名空间以及表
        try {
            HbaseDDL.createNamespace("weibo");//创建命名空间
            if(HbaseDDL.isTableExists("weibo","user")){
                //创建user表
                HbaseDDL.createTable("weibo","user","Basic","Relation");
            }
            if(HbaseDDL.isTableExists("weibo","weibo")){
                //创建weibo表
                HbaseDDL.createTable("weibo","weibo","Basic","count");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void storeUserData(){
        ResultSet rs=null;
        Connection con=null;
        Statement statement=null;
        try {
            //获取mysql连接
            JDBCUtils.getConnection();
            System.out.println("connection:"+con.isClosed());
            statement=con.createStatement();
            String sql="select * from user";
            rs = statement.executeQuery(sql);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("连接出错");
            return;
        }

        //user表属性
        String id;
        String screen_name;
        String gender;
        int statuses_count;
        int followers_count;
        int follow_count;
        String description;
        String profile_url;
        String profile_image_url;
        String avatar_hd;
        int urank;
        int mbrank;
        int verified;
        int verified_type;
        String verified_reason;
        try {
            while(rs.next()){

                id=rs.getString("id");
                screen_name = rs.getString("screen_name");
                gender=rs.getString("gender");
                statuses_count=rs.getInt("statuses_count");
                followers_count=rs.getInt("followers_count");
                follow_count=rs.getInt("follow_count");
                description=rs.getString("description");
                profile_url=rs.getString("profile_url");
                profile_image_url=rs.getString("profile_image_url");
                avatar_hd=rs.getString("avatar_hd");
                urank=rs.getInt("urank");
                mbrank=rs.getInt("mbrank");
                verified=rs.getInt("verified");
                verified_type=rs.getInt("verified_type");
                verified_reason=rs.getString("verified_reason");

                try {
                    //调用静态方法存入Hbase
                    HbaseDML.putStringCell("weibo","user",id,"Basic","screen_name",screen_name);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","gender",gender);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","description",description);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","profile_url",profile_url);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","profile_image_url",profile_image_url);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","avatar_hd",avatar_hd);
                    HbaseDML.putIntCell("weibo","user",id,"Basic","urank",urank);
                    HbaseDML.putIntCell("weibo","user",id,"Basic","mbrank",mbrank);
                    HbaseDML.putIntCell("weibo","user",id,"Basic","verified",verified);
                    HbaseDML.putIntCell("weibo","user",id,"Basic","verified_type",verified_type);
                    HbaseDML.putStringCell("weibo","user",id,"Basic","verified_reason",verified_reason);
                    HbaseDML.putIntCell("weibo","user",id,"Relation"," statuses_count", statuses_count);
                    HbaseDML.putIntCell("weibo","user",id,"Relation","followers_count",followers_count);
                    HbaseDML.putIntCell("weibo","user",id,"Relation","follow_count",follow_count);

                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("存入信息出错");
                    return;
                }


            }
            if(rs!=null){
                rs.close();
            }

            if (con!=null){
                con.close();
            }
            if (statement!=null){
                statement.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }



    }



    public static void storeWeiboData(){
        ResultSet rs=null;
        Connection con=null;
        Statement statement=null;
        try {
            //获取mysql连接
            JDBCUtils.getConnection();
            System.out.println("connection:"+con.isClosed());
            statement=con.createStatement();
            String sql="select * from weibo";
            rs = statement.executeQuery(sql);

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("连接出错");
            return;
        }

        //user表属性
        String id;
        String bid;
        String user_id;
        String screen_name;
        String topics;
        String at_users;
        String pics;
        String video_url;
        String location;
        String created_at;
        String source;
        int attitudes_count;
        int comments_count;
        int reposts_count;
        String retweet_id;
        try {
            while(rs.next()){
                id=rs.getString("id");
                bid=rs.getString("bid");
                user_id=rs.getString("user_id");
                screen_name=rs.getString("screen_name");
                topics=rs.getString("topics");
                at_users=rs.getString("at_users");
                pics=rs.getString("pics");
                video_url=rs.getString("video_url");
                location=rs.getString("location");
                created_at=rs.getString("created_at");
                source=rs.getString("source");
                attitudes_count=rs.getInt("attitudes_count");
                comments_count=rs.getInt("comments_count");
                reposts_count=rs.getInt("reposts_count");
                retweet_id=rs.getString(" retweet_id");


                try {;
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","id",id);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","bid",bid);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","user_id",user_id);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","screen_name",screen_name);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","topics",topics);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","at_users",at_users);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","pics",pics);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","video_url",video_url);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","location",location);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","created_at",created_at);
                    HbaseDML.putStringCell("weibo","weibo",id,"Basic","source",source);
                    HbaseDML.putIntCell("weibo","weibo",id ,"Basic","attitudes_count",attitudes_count);
                    HbaseDML.putIntCell("weibo","weibo",id ,"Basic","comments_count",comments_count);
                    HbaseDML.putIntCell("weibo","weibo",id ,"Basic","reposts_count",reposts_count);
                    HbaseDML.putStringCell("weibo","weibo",id ,"Basic","retweet_id",retweet_id);

                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("存入信息出错");
                    return;
                }


            }
            if(rs!=null){
                rs.close();
            }

            if (con!=null){
                con.close();
            }
            if(statement!=null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}


