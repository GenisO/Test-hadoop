import java.util.Collections;
import java.util.LinkedList;

/**
 * Created by DEIM on 22/09/14.
 */
public class DataXceiverServer {
  private DWRRManager dwrrmanager = new DWRRManager(new Configuration());

  public static void main(String[] args) {
    DataXceiverServer dXc = new DataXceiverServer();

    while (true) {
      dXc.doRequests();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }

  public void doRequests() {
    Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
      new Runnable() {
        @Override
        public void run() {
          LinkedList<Long> listClassId = new LinkedList<Long>();
          int pet = (int) (Math.random() * 15);
          while (listClassId.size() < pet) {
            listClassId.add((long) (Math.random() * 5));
          }

          for (Long classId : listClassId) {
            new DataXceiverDWRR(dwrrmanager, classId).processOp();
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          Collections.shuffle(listClassId);
          for (Long classId : listClassId) {
            new DataXceiverDWRR(dwrrmanager, classId).processOp();
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      });
    threadedDWRR.start();
  }


}
