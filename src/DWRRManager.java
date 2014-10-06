import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by DEIM on 28/07/14.
 */
public class DWRRManager {
  //public static final Log LOG = LogFactory.getLog(DWRRManager.class);
  private long[] testingWeight = new long[]{100, 100, 200, 300, 100, 500, 100, 400, 200, 100};//, 600, 400, 100, 300, 200, 100};
  private Object lock = new Object();
  private Configuration conf;
  private int numQueues;
  private int processedNumQueues;
  private long quantumSize;
  private Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
    new Runnable() {
      //public final Log LOG = LogFactory.getLog(Daemon.class);

      @Override
      public void run() {
        while (true) {

          synchronized (lock) {
            try {
              while (allRequestsQueue.size() == 0) {
                lock.wait();
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          WeightQueue<RequestObject> queue = allRequestsQueue.peek();
          while (allRequestsQueue.size() > 0 && queue.numPendingRequests() > 0) {
            System.out.println("CAMAMILLA while process requests init allRequestsQueue.size " + allRequestsQueue.size() + " queue.numPendingRequests " + queue.numPendingRequests());        // TODO TODO log
            RequestObject request = queue.peek();
            long queueDeficitCounter = queue.getDeficitCounter();
            if (queue.isNewRound()) {
              queue.setNewRound(false);
              queueDeficitCounter = queueDeficitCounter + quantumSize * queue.getWeight() / currentWeights.peek();
              queue.setDeficitCounter(queueDeficitCounter);
            }
            long requestSize = request.getSize();
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
              queue.updateProcessedBytes(requestSize);
              queue.updateProcessedRequests();
              System.out.println("CAMAMILLA INI print");
              for (long key : allRequestMap.keySet()) {
                WeightQueue<RequestObject> queueAux = allRequestMap.get(key);
                System.out.println("CAMAMILLA " + queueAux.toString());
              }
              System.out.println("Peticions totals " + numQueues + "\nCAMAMILLA END print");
              if (queue.numPendingRequests() == 0) {      // cua servida
                System.out.println("CAMAMILLA while process numPendingRequests = 0");        // TODO TODO log
                allRequestsQueue.poll();
                processedNumQueues++;
                System.out.println("CAMAMILLA " + numQueues + " cua " + queue.getClassId() + " buida ");        // TODO TODO log
                queue.setEndOrder(processedNumQueues);
                currentWeights.remove(queue.getWeight());
                System.exit(0);
                queue = allRequestsQueue.peek();    // Seleccionar seguent
              }

            } else {      // Augmentar deficitCounter i encuar i mirar seguent cua
              System.out.println("CAMAMILLA while process Augmentar deficitCounter");        // TODO TODO log
              //queueDeficitCounter += quantumSize;
              allRequestsQueue.poll();

              System.out.println("CAMAMILLA while process encara te peticions, tornar a encuar");        // TODO TODO log
              queue.setNewRound(true);
              allRequestsQueue.add(queue);

              //queue.setDeficitCounter(queueDeficitCounter);

              queue = allRequestsQueue.peek();
            }
          }
        }
      }
    });
  private int Ninit = 7;
  private PriorityQueue<Long> currentWeights;
  private Comparator<Long> maxComparator = new MaxNumericComparator();
  private Map<Long, WeightQueue<RequestObject>> allRequestMap;
  private Queue<WeightQueue<RequestObject>> allRequestsQueue;

  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavorim la reutilitzacio de codi
  public DWRRManager(Configuration conf) {
    this.conf = conf;
    this.allRequestsQueue = new ConcurrentLinkedQueue<WeightQueue<RequestObject>>();
    this.allRequestMap = new ConcurrentHashMap<Long, WeightQueue<RequestObject>>();
    //this.quantumSize = conf.getLong(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT);
    this.numQueues = 0;
    this.processedNumQueues = 0;
    this.currentWeights = new PriorityQueue<Long>(Ninit, maxComparator);
    this.quantumSize = 33554432;
    //System.out.println("CAMAMILLA DWRRManager init size block " + DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT + " quantumsize " + quantumSize);      // TODO TODO log
    //this.threadedDWRR.start();
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

    synchronized (lock) {
      //System.out.println("CAMAMILLA addop " + classId);      // TODO TODO log
      if (allRequestMap.get(classId) == null) {
        System.out.println("CAMAMILLA addop " + classId + " no al map");      // TODO TODO log
        long weight = (allRequestMap.keySet().size() < testingWeight.length ? testingWeight[allRequestMap.keySet().size()] : 1000);
        currentRequestQueue = new WeightQueue<RequestObject>(classId, weight, System.currentTimeMillis());
        allRequestMap.put(classId, currentRequestQueue);
      } else {
        System.out.println("CAMAMILLA addop " + classId + " al map");      // TODO TODO log
        currentRequestQueue = allRequestMap.get(classId);
      }

      currentRequestQueue.add(rec);
      if (currentRequestQueue.numPendingRequests() == 1) {
        allRequestsQueue.add(currentRequestQueue);
        currentWeights.add(currentRequestQueue.getWeight());
        numQueues++;
      }

      System.out.println("CAMAMILLA peticio " + classId + " encuada. Quantes peticions te: " + currentRequestQueue.size());      // TODO TODO log
      lock.notify();
    }
  }

  public void start() {
    this.threadedDWRR.start();
  }

  private class MaxNumericComparator implements Comparator<Long> {
    @Override
    public int compare(Long o1, Long o2) {
      if (o1 < o2) {
        return 1;
      } else if (o1 > o2) {
        return -1;
      }
      return 0;
    }

  }

}
