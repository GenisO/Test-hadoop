/**
 * Created by DEIM on 22/09/14.
 */
public class DataXceiverDWRR {
  private Long classId;
  private DWRRManager dwrrmanager;

  public DataXceiverDWRR(DWRRManager dwrrmanager, Long next) {
    this.dwrrmanager = dwrrmanager;
    this.classId = next;
  }

  public void makeOp(Op op) {
    switch (op) {
      case READ_BLOCK:
        System.out.println("CAMAMILLA makeReadBlock(proto) " + classId);
        break;
      case WRITE_BLOCK:
        System.out.println("CAMAMILLA makeWriteBlock() " + classId);
        break;
      default:
        System.out.println("ERROR makeOp " + op.code);      // TODO TODO log
    }
  }

  public void processOp() {
    int size = Op.values().length;
    Op[] values = Op.values();
    Op op = values[((int) (Math.random() * size))];
    long len = (long) (Math.random() * 100663296 + 33554432);
    RequestObject req = new RequestObject(this, classId, op, len);
    dwrrmanager.addOp(req, classId);
  }
}
