import org.apache.hadoop.util.Daemon;

import java.util.LinkedList;

/**
 * Created by DEIM on 21/10/14.
 */
public class Test {


  public static void main(String[] args) {
//    HDFSDWRRTest testClientDWRR = new HDFSDWRRTest();
//    String fileName = "big_file.txt";
//    String localPath = "/home/hadoop/hadoop-dir/";
//    String hadoopPath = "/";
//    testClientDWRR.setDestinationFilename(hadoopPath + fileName);
//    testClientDWRR.setSourceFilename(localPath + fileName);
//    testClientDWRR.uploadFile();
    Object lock = new Object();
    LinkedList<String> files = new LinkedList<String>();
    files.add("in100/file50Mb.txt");
    files.add("in200/file50Mb.txt");
    files.add("in400/file50Mb.txt");

    for (final String file : files) {
      Daemon threadRead = new Daemon(new ThreadGroup(
        "DWRR Thread"), new Runnable() {
        @Override
        public void run() {
          HDFSDWRRTest testClientDWRR = new HDFSDWRRTest();
          String fileName = file;
          String hadoopPath = "/";

          testClientDWRR.setSourceFilename(hadoopPath + fileName);
          testClientDWRR.downloadFile();
        }
      });
      threadRead.start();

    }

    synchronized (lock) {
      try {
        lock.wait();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }



  }
}
