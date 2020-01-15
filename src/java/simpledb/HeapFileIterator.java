package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private final int tableid;       
    private final int pageNum;
    private final TransactionId tid;
    private int currentPageNo;
    private Iterator<Tuple> currentPageIterator;
    private boolean open;
    private Tuple next;

    public HeapFileIterator(int tableid, int pageNum, TransactionId tid){
        this.tableid = tableid;
        this.pageNum = pageNum;
        this.tid = tid; 
        this.open = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
        rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return open && next != null;
    }

    public Iterator<Tuple> getPageIterator(int currentPageNo) throws TransactionAbortedException, DbException {
        Page page = Database.getBufferPool()
            .getPage(tid,new HeapPageId(tableid, currentPageNo), Permissions.READ_ONLY);
        return ((HeapPage)page).iterator();
    }
    
    public Tuple nextTuple() throws DbException, TransactionAbortedException, NoSuchElementException {
        while(currentPageNo < pageNum){
            if(currentPageIterator.hasNext())
                return currentPageIterator.next();
            currentPageNo ++;
            if(currentPageNo < pageNum){
                currentPageIterator = getPageIterator(currentPageNo);
            }
        }
        return null ;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(!open || !hasNext()) 
            throw new NoSuchElementException();
        Tuple tuple = next;
        next = nextTuple();
        return tuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if(open){
            currentPageNo = 0;
            currentPageIterator = getPageIterator(currentPageNo);
            next = nextTuple();
        }
    }

    @Override
    public void close() {
        open = false;
    }

}