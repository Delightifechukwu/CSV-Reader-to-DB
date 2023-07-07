import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static java.lang.Integer.parseInt;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException {

        String url= "jdbc:mysql://localhost:3306/schools";
        String uName = "root";
        String uPass= "Delight@193";
        String filepath= "C:\\Users\\HP\\OneDrive\\Desktop\\schools.csv";
        int batchsize=20;
        Connection con=null;
        try{
            //Class.forName("com.mysql.jdbc.Driver");
            con=DriverManager.getConnection(url,uName,uPass);
            con.setAutoCommit(false);
            String sql="INSERT INTO `schools`.`table` (`schoolid`, `schoolname`, `school location`, `schoolratings`) VALUES (?,?,?,?)";
            PreparedStatement state=con.prepareStatement(sql);
            BufferedReader read=new BufferedReader(new FileReader(filepath));
            String text=null;
            int count =0;
            read.readLine();
            while((text=read.readLine())!=null){
                String []data=text.split(",");
                String schoolid=data[0];
                String schoolname=data[1];
                String schoollocation=data[2];
                String schoolratings=data[3];
                state.setInt(1,parseInt(schoolid));
                state.setString(2,schoolname);
                state.setString(3, schoollocation);
                state.setString(4, schoolratings);
                state.addBatch();
                if(count%batchsize==0){
                    state.executeBatch();
                }

            }
            read.close();
            state.executeBatch();
            con.commit();
            con.close();
            System.out.println("the csv file has been inserted successfully");
        }catch(Exception e){
            System.out.println(e.getMessage());

        }

    }
}