import com.study.database.util.JDBCUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class connection {
    public static void main(String[] args){
        try {

            Connection con=JDBCUtils.getConnection();
            System.out.println("connection:"+con.isClosed());
            Statement statement=con.createStatement();
            String sql="select * from user";
            ResultSet rs = statement.executeQuery(sql);
            String id;
            String screen_name;
            String gender;
            int statuses_count;
            int followers_count;
            int follow_count;
            String registration_time;
            String sunshine;
            String birthday;
            String location;
            String education;
            String company;
            String description;
            String profile_url;
            String profile_image_url;
            String avatar_hd;
            int urank;
            int mbrank;
            int verified;
            int verified_type;
            String verified_reason;

            while(rs.next()){
                id=rs.getString("id");
                screen_name = rs.getString("screen_name");
                gender=rs.getString("gender");
                statuses_count=rs.getInt("statuses_count");
                followers_count=rs.getInt("followers_count");
                follow_count=rs.getInt("follow_count");
                registration_time=rs.getString("registration_time");
                sunshine= rs.getString("sunshine");
                birthday=rs.getString("birthday");
                location=rs.getString("location");
                education=rs.getString("education");
                company=rs.getString("company");
                description=rs.getString("description");
                profile_url=rs.getString("profile_url");
                profile_image_url=rs.getString("profile_image_url");
                avatar_hd=rs.getString("avatar_hd");
                urank=rs.getInt("urank");
                mbrank=rs.getInt("mbrank");
                verified=rs.getInt("verified");
                verified_type=rs.getInt("verified_type");
                verified_reason=rs.getString("verified_reason");

                System.out.println(id);
                System.out.println(screen_name);
                System.out.println(gender);
                System.out.println(statuses_count);
                System.out.println(followers_count);
                System.out.println(follow_count);
                System.out.println(registration_time);
                System.out.println(sunshine);
                System.out.println(birthday);
                System.out.println(location);
                System.out.println(education);
                System.out.println(company);
                System.out.println(description);
                System.out.println(profile_url);
                System.out.println(profile_image_url);
                System.out.println(avatar_hd);
            }
            rs.close();
            con.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
