package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    private OpIterator child;
    private int tableId;
    private final TransactionId tid;
    boolean called;
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.called = false;
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        if (!child.getTupleDesc().equals(file.getTupleDesc())) {
            throw new DbException("Tupledesc unmatch");
        }

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
        called = false;
        child.close();
        child.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor.
     * It returns a one field tuple containing the number of inserted records.
     * Inserts should be passed through BufferPool. An instances of BufferPool is
     * available via Database.getBufferPool(). Note that insert DOES NOT need check
     * to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if
     *         called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        BufferPool bufferPool = Database.getBufferPool();
        if(called) return null;
        else called = true;
        int counter = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                bufferPool.insertTuple(tid, tableId, tuple);
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
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
        
    }
}
