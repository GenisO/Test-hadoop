import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Created by DEIM on 28/07/14.
 */
public class DWRRManager {
  //  public static final Log LOG = LogFactory.getLog(DWRRManager.class);
  private final long NULL_SIZE = 0;
  private Configuration conf;
  private int numQueues;
  private long quantumSize;
  private Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
    new Runnable() {
      //      public final Log LOG = LogFactory.getLog(Daemon.class);
      @Override
      public void run() {
        while (true) {

          synchronized (this) {
            try {
              System.out.println("CAMAMILLA Thread dormit");
              this.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          System.out.println("CAMAMILLA Thread despert");

          while (allRequestsQueue.size() > 0) {
            WeightQueue<RequestObject> queue = allRequestsQueue.peek();

            while (queue.numPendingRequests() > 0 /* && queue.getDeficitCounter() > quantumSize*/) {
              RequestObject request = queue.peek();
              long requestSize = request.getSize();
              long queueDeficitCounter = queue.getDeficitCounter();
              if (requestSize <= queueDeficitCounter) {     // Processar petico
                queueDeficitCounter -= requestSize;
                queue.setDeficitCounter(queueDeficitCounter);
                request = queue.poll();
                System.out.println("CAMAMILLA " + request.getClassId() + " Thread processar peticio " + request.getOp());        // TODO TODO log
                try {
                  DataXceiverDWRR dXc = request.getdXc();
                  dXc.makeOp(request.getOp());
                } catch (Exception e) {
                  System.out.println("CAMAMILLA " + request.getClassId() + " Thread DWRRManager peta " + e);        // TODO TODO log
                }
              } else {      // Augmentar deficitCounter i encuar i mirar seguent cua
                queueDeficitCounter += quantumSize;
                queue.setDeficitCounter(queueDeficitCounter);
                break;
              }
            }

            queue = allRequestsQueue.poll();
            if (queue.numPendingRequests() > 0) {   // Si encara te peticions, tornar a encuar, sino totes les peticions ja estan servides i es descarta
              allRequestsQueue.add(queue);
            }
          }
        }
      }
    });
  private long currentId;
  private Map<Long, WeightQueue<RequestObject>> allRequestQueueReferences;
  private Queue<WeightQueue<RequestObject>> allRequestsQueue;


  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavarim la reutilitzacio de codi
  public DWRRManager(Configuration conf) {
    this.allRequestsQueue = new LinkedList<WeightQueue<RequestObject>>();
    this.allRequestQueueReferences = new HashMap<Long, WeightQueue<RequestObject>>();
    this.currentId = -1;
    this.conf = conf;
    this.numQueues = 0;

    // fer un objecte gros amb tot el de les cues,
    // i la gestio de les cues que tambe sigui una cua, i anem encuant i desencuant
    // no eliminar les peticons servides, per tant shan de marcar com a fetes

//    this.quantumSize = conf.getLong(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT);
    this.quantumSize = 12;

//    System.out.println("CAMAMILLA DWRRManager init size block "+DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT+" quantumsize "+quantumSize);      // TODO TODO log
    this.threadedDWRR.start();
  }

  /**
   * Queue
   * boolean 		offer(E e)			Inserts the specified element at the tail of this queue if it is possible to do so immediately without exceeding the queue's capacity, returning true upon success and false if this queue is full.
   * boolean		offer(E e, long timeout, TimeUnit unit)			Inserts the specified element at the tail of this queue, waiting if necessary up to the specified wait time for space to become available.
   * void				put(E e)			Inserts the specified element at the tail of this queue, waiting if necessary for space to become available.
   * E					peek()			Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
   * E					poll()			Retrieves and removes the head of this queue, or returns null if this queue is empty.
   * E					poll(long timeout, TimeUnit unit)			Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element to become available.
   */
  public void addOp(RequestObject rec, long classId) {
    WeightQueue<RequestObject> currentRequestQueue;
    boolean wakeUpThread = false;
    if (allRequestsQueue.size() == 0) {
      wakeUpThread = true;
    }
    if (allRequestQueueReferences.get(classId) == null) {
      currentRequestQueue = new WeightQueue<RequestObject>(classId, 1);
      allRequestQueueReferences.put(classId, currentRequestQueue);
      allRequestsQueue.add(currentRequestQueue);
      numQueues++;
    } else {
      currentRequestQueue = allRequestQueueReferences.get(classId);
    }

    currentRequestQueue.add(rec);

    System.out.println("CAMAMILLA peticio " + classId + " encuada. Quantes peticions te: " + currentRequestQueue.size());      // TODO TODO log
    if (wakeUpThread) {
      synchronized (threadedDWRR.getRunnable()) {
        threadedDWRR.getRunnable().notify();
      }
    }

  }

}
