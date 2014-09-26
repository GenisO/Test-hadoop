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
  private int lvl;
  private long quantumSize;
  private long currentId;
  private Map<Long, WeightQueue<RequestObject>> referenceRequestQueue;
  private Queue<WeightQueue<RequestObject>> requestsQueue;

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

          RequestObject rec = getReqObject();
          if (rec != null) {
            System.out.println("CAMAMILLA " + rec.getClassId() + " Thread processar peticio " + rec.getOp());        // TODO TODO log
            try {
              DataXceiverDWRR dXc = rec.getdXc();
              dXc.makeOp(rec.getOp());
            } catch (Exception e) {
              System.out.println("CAMAMILLA " + rec.getClassId() + " Thread DWRRManager peta " + e);        // TODO TODO log
            }
          }

        }
      }
    });


  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavarim la reutilitzacio de codi
  public DWRRManager(Configuration conf) {
    this.requestsQueue = new LinkedList<WeightQueue<RequestObject>>();
    this.referenceRequestQueue = new HashMap<Long, WeightQueue<RequestObject>>();
    this.currentId = -1;
    this.conf = conf;
    this.numQueues = 0;
    this.lvl = 0;

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
    WeightQueue<RequestObject> reqQueue;

    if (referenceRequestQueue.get(classId) == null) {
      reqQueue = new WeightQueue<RequestObject>(classId, 1);
      referenceRequestQueue.put(classId, reqQueue);
      requestsQueue.add(reqQueue);
      numQueues++;
    } else {
      reqQueue = referenceRequestQueue.get(classId);
    }

    reqQueue.add(rec);

    System.out.println("CAMAMILLA peticio " + classId + " encuada. Quantes peticions te: " + reqQueue.size());      // TODO TODO log
    synchronized (threadedDWRR.getRunnable()) {
      threadedDWRR.getRunnable().notify();
    }
  }

  public RequestObject getReqObject() {
    RequestObject rec = null;
    if (lvl <= numQueues) {
      WeightQueue<RequestObject> reqQueue = requestsQueue.peek();
      if (reqQueue != null) {
        rec = reqQueue.peek();
        if (rec == null) {
          if (requestsQueue.size() > 1) {
            requestsQueue.add(requestsQueue.poll());
            lvl++;
            return getReqObject();
          }
        } else {
          long defCount = reqQueue.getDeficitCounter();
          if (currentId == -1) {
            currentId = reqQueue.getClassId();
            defCount += quantumSize;
            reqQueue.setDeficitCounter(defCount);
          }
          long len = rec.getSize();
          if (len <= defCount) {
            defCount -= len;
            reqQueue.setDeficitCounter(defCount);
            return reqQueue.poll();
          } else {
            if (requestsQueue.size() > 1) {
              requestsQueue.add(requestsQueue.poll());
              lvl++;
              return getReqObject();
            }
          }
        }
      }
    }

    lvl = 0;
    return rec;
  }


}
