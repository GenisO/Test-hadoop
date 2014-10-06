import java.util.LinkedList;
import java.util.Random;

/**
 * Created by DEIM on 22/09/14.
 */
public class DataXceiverServer {
  private static Random generadorNumerosAleatoris = new Random();
  private DWRRManager dwrrmanager = new DWRRManager(new Configuration());

  public static void main(String[] args) {
    DataXceiverServer dXc = new DataXceiverServer();

    dXc.doRequests(5000, 5000, 10);
//    for (int i = 1; i < 2; i++) {
//      double max = Math.random() * 5123 * i + 125 * (i + 2);
//      double min = Math.random() * (2000 + 2000/i);
//      if (max < min) dXc.doRequests(min, max, i);
//      else dXc.doRequests(max, min, i);
////      try {
////        Thread.sleep(1000);
////      } catch (InterruptedException e) {
////        e.printStackTrace();
////      }
//    }
    System.out.println("Fin injeccio de peticions");
    //dXc.start();
    while (true) {

    }
  }

  private void start() {
    dwrrmanager.start();
  }


  public void doRequests(final double max, final double min, final int numClassId) {
    Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
      new Runnable() {
        @Override
        public void run() {
          ;
          generadorNumerosAleatoris.setSeed(System.currentTimeMillis());

          LinkedList<Long> listClassId = new LinkedList<Long>();
          int numPeticions = 5000;
          while (listClassId.size() < numClassId) {
            listClassId.add((long) (generadorNumerosAleatoris.nextInt((int) min)));
          }

          for (Long classId : listClassId) {
            int peticions = numPeticions;
            while (peticions > 0) {
              new DataXceiverDWRR(dwrrmanager, classId).processOp();
              peticions--;
//            try {
//              Thread.sleep(100);
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
            }
          }
          start();
        }
      });
    threadedDWRR.start();

  }


}
