package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    private OpIterator child;
    private int tableId;
    private final TransactionId tid;

    boolean called;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        rewind();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        child.open();
        called = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        BufferPool bufferPool = Database.getBufferPool();
        if(called) return null;
        else called = true;
        int counter = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                bufferPool.deleteTuple(tid, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            counter ++ ;
        }
        Tuple tuple = new Tuple(td);
        tuple.setField(0, new IntField(counter));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return  new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
