/**
 * Created by DEIM on 21/10/14.
 */
public class Test {


  public static void main(String[] args) {
    HDFSDWRRTest testClientDWRR = new HDFSDWRRTest();
    String fileName = "big_file.txt";
    String localPath = "/home/hadoop/hadoop-dir/";
    String hadoopPath = "/";
    testClientDWRR.setDestinationFilename(localPath + fileName);
    testClientDWRR.setSourceFilename(hadoopPath);
    testClientDWRR.uploadFile();

    testClientDWRR.setSourceFilename(hadoopPath + fileName);
    testClientDWRR.downloadFile();

  }
}
