
import java.io.BufferedReader;
import java.io.FileNotFoundException;
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

import static java.lang.Integer.parseInt;

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
            //conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fileToDataBase(String filePath) throws SQLException, IOException {

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String currLine = "";
            currLine = br.readLine();
            while(currLine != null){
                String[] splitToInsert = currLine.split(",");
                PreparedStatement commands = this.conn.prepareStatement("INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES(?,?)");
                commands.setString(1,splitToInsert[0]);
                commands.setInt(2,parseInt(splitToInsert[1]));
                commands.executeUpdate();
                currLine = br.readLine();
            }


    }


    public void calculateSimilarity(){
        List<Long> filmsRecords = readAll();
        int distance = DistClculation();
        for(int i = 0 ; i < filmsRecords.size() ; i++){
            for (int j = (filmsRecords.size() - 1) ; j > i ;  j--){
                float similarity = SimilarityCalculation(filmsRecords.get(i), filmsRecords.get(j), distance);
                insertToSimilarityTable(filmsRecords.get(i), filmsRecords.get(j), similarity);
            }
        }
    }

    private  List<Long> readAll() {
        List<Long> allTheItems = new ArrayList<Long>();
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("SELECT MID FROM MEDIAITEMS");
            ResultSet r = ps.executeQuery();
            while ( r.next() ) {
                Long mid = r.getLong("MID");
                allTheItems.add(mid);

            }
            r.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return allTheItems;
    }

    private int DistClculation() {
        CallableStatement cs = null;
        int ans = -1;
        try {
            cs = this.conn.prepareCall("{? =call MaximalDistance}");
            cs.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
            cs.execute();
            ans = cs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                cs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return ans;
    }

    private float SimilarityCalculation(Long m1, Long m2, int dist) {
        float ans = -1;
        CallableStatement c = null;
        try {
            c = this.conn.prepareCall("{? =call SimCalculation(?,?,?)}");


            c.setLong(2, m1);
            c.setLong(3, m2);
            c.setInt(4, dist);
            c.registerOutParameter(1, oracle.jdbc.OracleTypes.FLOAT);
            c.execute();
            ans =  c.getFloat(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return ans;
    }


    private void insertToSimilarityTable(Long m1, Long m2, float calcAns) {
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement("INSERT INTO SIMILARITY (MID1, MID2, SIMILARITY) VALUES(?,?,?)");
            ps.setLong(1, m1);
            ps.setLong(2, m2);
            ps.setFloat(3, calcAns);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

