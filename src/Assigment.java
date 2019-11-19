
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Assigment {


    private Connection conn = null;
    private String connectionUrl = "";
    private String userName = "";
    private String password = "";
//    private final String driver = "oracle.jdbc.driver.OracleDriver";


    public Assigment(String connection, String uname, String pass) {
        this.userName = uname;
        this.password = pass;
        this.connectionUrl = connection;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver"); //registration of the driver
            this.conn = DriverManager.getConnection(connectionUrl, userName, password);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fileToDataBase(String filePath) {

        String read = "";
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while (null != (read = br.readLine())) {
                String[] csvOutput = read.split(",");

                if (null == this.conn) {
                    try {
                        Class.forName("oracle.jdbc.driver.OracleDriver"); //registration of the driver
                        this.conn = DriverManager.getConnection(connectionUrl, userName, password);
                        conn.setAutoCommit(false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                PreparedStatement p = null;
                String query = "INSERT INTO MEDIAITEMS(TITLE,PROD_YEAR)" +
                        " VALUES(?,?)";
                try {
                    p = conn.prepareStatement(query);
                    p.setString(1, csvOutput[0]);
                    p.setInt(2, Integer.parseInt(csvOutput[1]));
                    p.executeUpdate();
                    conn.commit();
                } catch (SQLException e) {
                    try {
                        conn.rollback();
                    } catch (SQLException e2) {
                        e2.printStackTrace();
                    }
                    e.printStackTrace();
                } finally {
                    try {
                        if (p != null) {
                            p.close();
                        }
                    } catch (SQLException e3) {
                        e3.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

//    public void disconnect()
//    {
//        try
//        {
//            this.conn.close();
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


