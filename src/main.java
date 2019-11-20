public class main {


    public static void main(String[]args) {
        Assigment shit = new Assigment("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE", "ilayfri", "ilayfRi140");
        shit.fileToDataBase("C:/Users/ilayfri.BGU-USERS/IdeaProjects/assignmentBD_1/films.csv");
    }


}
