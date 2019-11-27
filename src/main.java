import java.io.IOException;
import java.sql.SQLException;

public class main {


    public static void main(String[]args) throws IOException, SQLException {
        Assigment DB = new Assigment("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE", "ilayfri", "ilayfRi140");

        //DB.fileToDataBase("D:/documents/users/ilayfri/Downloads/films.csv");
        //DB.calculateSimilarity();
        DB.printSimilarItems(32);
    }


}
