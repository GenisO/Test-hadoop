import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClientDWRR;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.RequestObjectDWRR;
import org.apache.hadoop.hdfs.protocol.datatransfer.WeightQueueDWRR;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataXceiverDWRR;
import org.apache.hadoop.hdfs.server.namenode.ByteUtils;
import org.apache.hadoop.hdfs.server.namenode.FairIOControllerDWRR;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.hadoop.util.Time.now;

/**
 * Created by DEIM on 28/07/14.
 */
public class DWRRManager {
  public static final Log LOG = LogFactory.getLog(org.apache.hadoop.hdfs.protocol.datatransfer.DWRRManager.class);
  public static final String nameWeight = "weight";
  private DataNode datanode;
  private DFSClientDWRR dfs;
  private Object lock = new Object();
  private Configuration conf;
  private int numQueues;
  private int processedNumQueues;
  private long quantumSize;
  private boolean weigthedFairShare;
  private int Ninit = 7;
  private PriorityQueue<Float> currentActiveWeights;
  private Daemon threadedDWRR = new Daemon(new ThreadGroup("DWRR Thread"),
    new Runnable() {
      public final Log LOG = LogFactory.getLog(Daemon.class);

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

          WeightQueueDWRR<RequestObjectDWRR> queue = allRequestsQueue.peek();
          while (allRequestsQueue.size() > 0 && queue.numPendingRequests() > 0) {
            LOG.info("CAMAMILLA while process requests init allRequestsQueue.size " + allRequestsQueue.size() + " queue.numPendingRequests " + queue.numPendingRequests() + " of " + queue.getClassId());        // TODO TODO log
            LOG.info("CAMAMILLA while process requests init allRequestsQueue.array " + allRequestsQueue.iterator().toString());        // TODO TODO log
            RequestObjectDWRR request = queue.peek();
            float queueDeficitCounter = queue.getDeficitCounter();
            if (queue.isNewRound()) {
              LOG.info("CAMAMILLA while process requests is new round for " + queue.getClassId());        // TODO TODO log
              queue.setNewRound(false);
              if (!weigthedFairShare) queueDeficitCounter += quantumSize;
              else {
                LOG.info("CAMAMILLA while calcul deficitCounter = " + queueDeficitCounter + " + " + quantumSize + " * " + queue.getWeight() + " / " + maxWeight());        // TODO TODO log
                queueDeficitCounter += quantumSize * queue.getWeight() / maxWeight();
              }
              queue.setDeficitCounter(queueDeficitCounter);
            }

            long requestSize = request.getSize();
            LOG.info("CAMAMILLA while queda prou deficitCounter? " + requestSize + " <= " + queueDeficitCounter + " ?");        // TODO TODO log
            if (requestSize <= queueDeficitCounter) {     // Processar petico
              queueDeficitCounter -= requestSize;
              queue.setDeficitCounter(queueDeficitCounter);
              request = queue.peek();

              LOG.info("CAMAMILLA " + request.getClassId() + " Thread processar peticio " + request.getOp());        // TODO TODO log
              try {
                DataXceiverDWRR dXc = request.getdXc();
                dXc.makeOp(request.getOp());
              } catch (Exception e) {
                LOG.info("CAMAMILLA " + request.getClassId() + " Thread DWRRManager peta " + e);        // TODO TODO log
              }

              queue.updateProcessedRequests();
              queue.updateProcessedBytes(requestSize);
              queue.setEndOrder(request);

              datanode.getMetrics().incrProcessedRequest("" + queue.getClassId(), requestSize, queue.getWeight());

              synchronized (lock) {
                queue.poll();
                if (queue.numPendingRequests() == 0) {      // cua servida
                  LOG.info("CAMAMILLA while process numPendingRequests = 0");        // TODO TODO log
                  queue.setDeficitCounter(0F);
                  allRequestsQueue.poll();
                  processedNumQueues++;
                  LOG.info("CAMAMILLA " + numQueues + " cua " + queue.getClassId() + " buida " + " amb peticions servides= " + queue.getProcessedRequests());        // TODO TODO log
                  String weights = "";
                  for (float weight : currentActiveWeights) {
                    weights += " " + weight;
                  }

                  LOG.info("CAMAMILLA DWRRManager.addOp pesos abans deliminar " + queue.getWeight() + " son {" + weights + "}");      // TODO TODO log

                  currentActiveWeights.remove(queue.getWeight());

                  weights = "";
                  for (float weight : currentActiveWeights) {
                    weights += " " + weight;
                  }

                  LOG.info("CAMAMILLA DWRRManager.addOp pesos despres deliminar " + queue.getWeight() + " son {" + weights + "}");      // TODO TODO log

                  numQueues--;
                  queue = allRequestsQueue.peek();    // Seleccionar seguent
                }
              }

            } else {      // Augmentar deficitCounter i encuar i mirar seguent cua
              LOG.info("CAMAMILLA while process Augmentar deficitCounter");        // TODO TODO log
              allRequestsQueue.poll();

              LOG.info("CAMAMILLA while process encara te peticions, tornar a encuar");        // TODO TODO log
              queue.setNewRound(true);
              allRequestsQueue.add(queue);

              queue = allRequestsQueue.peek();
            }
            LOG.info("CAMAMILLA INI print");          // TODO TODO log
            for (long key : allRequestMap.keySet()) {
              WeightQueueDWRR<RequestObjectDWRR> queueAux = allRequestMap.get(key);
              LOG.info(now() + "CAMAMILLA " + queueAux.toString());          // TODO TODO log
            }
            String weights = "";
            for (float weight : currentActiveWeights) {
              weights += " " + weight;
            }
            LOG.info("CAMAMILLA DWRRManager.Thread run pesos son {" + weights + "}");      // TODO TODO log
            LOG.info("CAMAMILLA END print");          // TODO TODO log
          }
        }
      }
    });
  private Comparator<Float> maxComparator = Collections.reverseOrder();
  private Map<Long, WeightQueueDWRR<RequestObjectDWRR>> allRequestMap;
  private Queue<WeightQueueDWRR<RequestObjectDWRR>> allRequestsQueue;

  // TODO TODO fer que totes les classes propies que siguin modificacio duna altra de hadoop siguin per herencia, aixi afavorim la reutilitzacio de codi
  public DWRRManager(Configuration conf, DFSClientDWRR dfs, DataNode datanode) {
    this.conf = conf;
    this.allRequestsQueue = new ConcurrentLinkedQueue<WeightQueueDWRR<RequestObjectDWRR>>();
    this.allRequestMap = new ConcurrentHashMap<Long, WeightQueueDWRR<RequestObjectDWRR>>();
    this.quantumSize = conf.getLong(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT);
    this.weigthedFairShare = conf.getBoolean(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_WEIGTHED_FAIR_SHARE, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_WEIGTHED_FAIR_SHARE_DEFAULT);
    this.numQueues = 0;
    this.processedNumQueues = 0;
    this.currentActiveWeights = new PriorityQueue<Float>(Ninit, maxComparator);
    this.dfs = dfs;
    this.datanode = datanode;

    //LOG.info("CAMAMILLA DWRRManager init size block " + DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_QUANTUM_SIZE_DEFAULT + " quantumsize " + quantumSize);      // TODO TODO log
    this.threadedDWRR.start();
  }

  private Float maxWeight() {
    return currentActiveWeights.peek();
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
  public void addOp(RequestObjectDWRR rec, long classId) {
    synchronized (lock) {
      WeightQueueDWRR<RequestObjectDWRR> currentRequestQueue;

      if (allRequestMap.get(classId) == null) {
        LOG.info("CAMAMILLA addop " + classId + " no al map");      // TODO TODO log

        Map<String, byte[]> xattr = null;
        float weight;
        try {
          xattr = dfs.getXAttrs(classId, datanode.getDatanodeId().getDatanodeUuid());

          LOG.info("CAMAMILLA DataXceiverDWRR.opReadBlock.list " + classId + " xattr size " + xattr.keySet().size());      // TODO TODO log
          for (String k : xattr.keySet()) {
            LOG.info("CAMAMILLA DataXceiverDWRR.opReadBlock.list xattr " + k + ":" + ByteUtils.bytesToFloat(xattr.get(k)));      // TODO TODO log
          }

          if (xattr == null) {
            LOG.error("CAMAMILLA DataXceiverDWRR.opReadBlock.list no te atribut weight");      // TODO TODO log
            weight = FairIOControllerDWRR.DEFAULT_WEIGHT;
          } else {
            LOG.error("CAMAMILLA DataXceiverDWRR.opReadBlock.list fer el get de user." + org.apache.hadoop.hdfs.protocol.datatransfer.DWRRManager.nameWeight);      // TODO TODO log
            weight = ByteUtils.bytesToFloat(xattr.get("user." + org.apache.hadoop.hdfs.protocol.datatransfer.DWRRManager.nameWeight));
          }
        } catch (IOException e) {
          LOG.error("CAMAMILLA DataXceiverDWRR.opReadBlock.list ERROR al getXattr " + e.getMessage());      // TODO TODO log
          weight = FairIOControllerDWRR.DEFAULT_WEIGHT;
        }

        currentRequestQueue = new WeightQueueDWRR<RequestObjectDWRR>(classId, weight, System.currentTimeMillis());
        allRequestMap.put(classId, currentRequestQueue);
      } else {
        LOG.info("CAMAMILLA addop " + classId + " al map");      // TODO TODO log
        currentRequestQueue = allRequestMap.get(classId);
      }

      currentRequestQueue.add(rec);
      if (!allRequestsQueue.contains(currentRequestQueue)) {
        allRequestsQueue.add(currentRequestQueue);

        String weights = "";
        for (float weight : currentActiveWeights) {
          weights += " " + weight;
        }

        LOG.info("CAMAMILLA DWRRManager.addOp pesos abans dafegir " + currentRequestQueue.getWeight() + " son {" + weights + "}");      // TODO TODO log

        currentActiveWeights.add(currentRequestQueue.getWeight());

        weights = "";
        for (float weight : currentActiveWeights) {
          weights += " " + weight;
        }

        LOG.info("CAMAMILLA DWRRManager.addOp pesos despres dafegir " + currentRequestQueue.getWeight() + " son {" + weights + "}");      // TODO TODO log

        numQueues++;
      }

      LOG.info("CAMAMILLA peticio " + classId + " encuada amb pes " + currentRequestQueue.getWeight() + ". Quantes peticions te: " + currentRequestQueue.size());      // TODO TODO log
      lock.notify();
    }
  }

}