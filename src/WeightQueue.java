import java.util.*;

/**
 * Created by DEIM on 26/09/14.
 */
public class WeightQueue<E extends RequestObject> implements Queue<E> {

  private long insertionTime;
  private long processedBytes;
  private Queue<E> requests;
  private long deficitCounter;
  private long weight;
  private int processedRequests;
  private int totalRequests;
  private long classId;
  private boolean newRound;
  private List<Integer> order;


  public WeightQueue(long classId, long weight, long time) {
    this.weight = weight;
    this.classId = classId;
    this.deficitCounter = 0;
    this.requests = new LinkedList<E>();
    this.processedRequests = 0;
    this.totalRequests = 0;
    this.processedBytes = 0;
    this.newRound = true;
    this.insertionTime = time;
    this.order = new LinkedList<Integer>();
  }

  public long getProcessedBytes() {
    return processedBytes;
  }

  public void updateProcessedBytes(long processedBytes) {
    this.processedBytes += processedBytes;
  }

  public long getWeight() {
    return weight;
  }

  public long getClassId() {
    return classId;
  }

  public int getProcessedRequests() {
    return processedRequests;
  }

  public void updateProcessedRequests() {
    this.processedRequests++;
  }

  public long getDeficitCounter() {
    return deficitCounter;
  }

  public void setDeficitCounter(long defC) {
    this.deficitCounter = defC;
  }

  public int numPendingRequests() {
    return this.size();
  }

  @Override
  public String toString() {
    return "Insercio " + insertionTime + "\tOrdre " + order + "\t\t\t\tclassId " + classId + "\t\t\t\tpes " + weight + "\t\t\t\tdeficitCounter " + deficitCounter + "\t\t\t\t" + processedRequests + ":" + totalRequests + "\t\t\t\tprocessed bytes " + processedBytes;
  }

  @Override
  public int size() {
    return this.requests.size();
  }

  @Override
  public boolean isEmpty() {
    return this.requests.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.requests.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return this.requests.iterator();
  }

  @Override
  public Object[] toArray() {
    return this.requests.toArray();
  }

  @Override
  public <T> T[] toArray(T[] ts) {
    return this.requests.toArray(ts);
  }

  @Override
  public boolean add(E e) {
    this.totalRequests++;
    return this.requests.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return this.requests.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> objects) {
    return this.requests.containsAll(objects);
  }

  @Override
  public boolean addAll(Collection<? extends E> es) {
    return this.requests.addAll(es);
  }

  @Override
  public boolean removeAll(Collection<?> objects) {
    return this.requests.removeAll(objects);
  }

  @Override
  public boolean retainAll(Collection<?> objects) {
    return this.requests.retainAll(objects);
  }

  @Override
  public void clear() {
    this.requests.clear();
  }

  @Override
  public boolean offer(E e) {
    return this.requests.offer(e);
  }

  @Override
  public E remove() {
    return this.requests.remove();
  }

  @Override
  public E poll() {
    return this.requests.poll();
  }

  @Override
  public E element() {
    return this.requests.element();
  }

  @Override
  public E peek() {
    return this.requests.peek();
  }

  public boolean isNewRound() {
    return newRound;
  }

  public void setNewRound(boolean nR) {
    newRound = nR;
  }

  public void setEndOrder(int numQueues) {
    order.add(numQueues);
  }
}
