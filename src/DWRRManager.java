import java.util.*;

/**
 * Created by DEIM on 28/07/14.
 */
public class DWRRManager {
  private final DataXceiverServer dataXceiverServer;
  private final long NULL_SIZE = 0;
  private Iterator<Long> requestsIterator;
  private Map<Long, Queue<RequestObject>> requests;
  private Map<Long, Integer> processedRequests;
  private Integer quantumSize;
  private Map<Long, Integer> weights;
  private Map<Long, Long> deficitCounter;
  private boolean ho = true;
  private int numRequests;
  private int profunditat = 0;
  private long currentId;

  private Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
    new Runnable() {
      private int count = 0;

      @Override
      public void run() {
        while (true) {
          synchronized (this) {
            try {
              System.out.println("Thread dormit");
              this.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          System.out.println("Thread despert");

          count++;
          RequestObject rec = getReqObject();
          if (rec != null) {
            //System.out.println(" Thread " + rec.getOp() + " volta " + count + " numero de peticions enquades " + numRequests);        // TODO TODO log
//                            try {
            DataXceiverDWRR dXc = rec.getdXc();
            dXc.makeOp(rec.getOp());
          }


//                        try {
//                            Thread.sleep(50);
//                            if (count % 500 == 0) System.out.println("Thread DWRRManager ha dormit");        // TODO TODO log
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
        }
      }
    });


  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavarim la reutilitzacio de codi
  public DWRRManager(DataXceiverServer dataXceiverServer) {
    this.requests = new HashMap<Long, Queue<RequestObject>>();
    this.weights = new HashMap<Long, Integer>();
    this.deficitCounter = new HashMap<Long, Long>();
    this.processedRequests = new HashMap<Long, Integer>();
    this.requestsIterator = requests.keySet().iterator();
    this.dataXceiverServer = dataXceiverServer;
    this.currentId = -1;
    this.quantumSize = 65024;

    this.threadedDWRR.start();

  }

  /**
   * LinkedBlockingQueue
   * boolean 		offer(E e)			Inserts the specified element at the tail of this queue if it is possible to do so immediately without exceeding the queue's capacity, returning true upon success and false if this queue is full.
   * boolean		offer(E e, long timeout, TimeUnit unit)			Inserts the specified element at the tail of this queue, waiting if necessary up to the specified wait time for space to become available.
   * void				put(E e)			Inserts the specified element at the tail of this queue, waiting if necessary for space to become available.
   * E					peek()			Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
   * E					poll()			Retrieves and removes the head of this queue, or returns null if this queue is empty.
   * E					poll(long timeout, TimeUnit unit)			Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element to become available.
   */
  public void addOp(RequestObject rec, long classId) {
    if (requests.get(classId) == null) {
      requests.put(classId, new LinkedList<RequestObject>());
      deficitCounter.put(classId, NULL_SIZE);
      requestsIterator = requests.keySet().iterator();
    }

    Queue<RequestObject> list = requests.get(classId);
    list.add(rec);
    //requests.put(classId, list);

    numRequests++;
    System.out.println(" peticio " + classId + " encuada. Llista peticions " + list.size());      // TODO TODO log

    synchronized (threadedDWRR.getRunnable()) {
      threadedDWRR.getRunnable().notify();
    }
  }

  public RequestObject getReqObject() {
    RequestObject reqObj = null;
    profunditat = 0;
    long classIdTorn = whosTorn();
    if (classIdTorn != -1 && requests.get(classIdTorn) != null) {
      Queue<RequestObject> list = requests.get(classIdTorn);
      reqObj = list.poll();
      if (list.size() == 0) {
        requests.remove(classIdTorn);
        deficitCounter.put(currentId, NULL_SIZE);
        requestsIterator = requests.keySet().iterator();
        currentId = -1;
      }
    }

    System.out.println("ProcessOp classId " + classIdTorn + " de llista de classid " + requests.get(classIdTorn) + " i request " + reqObj);      // TODO TODO log
    return reqObj;
  }

  private long whosTorn() {
    System.out.println("whosTorn Profunditat " + profunditat + " currentId " + currentId);

    Long defCount;
    if (currentId == -1) {
      if (requestsIterator.hasNext()) {
        currentId = requestsIterator.next();
        defCount = deficitCounter.get(currentId);
        defCount += quantumSize;
        deficitCounter.put(currentId, defCount);
      } else return -1;
    }

    defCount = deficitCounter.get(currentId);
    Queue<RequestObject> poolReqs = requests.get(currentId);
    RequestObject reqOp = poolReqs.peek();
    System.out.println("whosTorn currentId " + currentId + " defCount " + defCount + " quantumSize " + quantumSize + " len " + reqOp.getSize());      // TODO TODO log
    if (reqOp.getSize() <= defCount) {
      defCount -= reqOp.getSize();
      deficitCounter.put(currentId, defCount);
      System.out.println("whosTorn Next currentId " + currentId + " defCount restant" + defCount);      // TODO TODO log
      return currentId;
    } else {
      System.out.println("whosTorn buscar nou currentId " + currentId + " tornar a preguntar");      // TODO TODO log
      currentId = -1;
      profunditat++;
      return whosTorn();
    }


  }

}
