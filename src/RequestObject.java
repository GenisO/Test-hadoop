
/**
 * Created by DEIM on 31/07/14.
 */
public class RequestObject {

    private long size;
    private Op op;
    private long classId;
    private DataXceiverDWRR dXc;

    public RequestObject(DataXceiverDWRR dataXceiverDWRR, long classId, Op op, long len) {
        this.dXc = dataXceiverDWRR;
        this.classId = classId;
        this.op = op;
        this.size = len;
    }

    public long getClassId() {
        return classId;
    }

    public void setClassId(long classId) {
        this.classId = classId;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
        this.op = op;
    }

    public DataXceiverDWRR getdXc() {
        return dXc;
    }

    public long getSize() {
        return size;
    }
}
