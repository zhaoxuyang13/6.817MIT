package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator { 

    private static final long serialVersionUID = 1L;
    private final  TransactionId tid;
    private DbFile file;
    private String name;
    private String tableAlias;
    private boolean open;
    private Tuple next;
    private DbFileIterator fIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableAlias = tableAlias;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.name = Database.getCatalog().getTableName(tableid);
        this.open = false;
        this.fIterator = file.iterator(this.tid);
        // fIterator.open();
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return name;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias(){
       return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableAlias = tableAlias;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.name = Database.getCatalog().getTableName(tableid);
        this.open = false;
        this.fIterator = file.iterator(this.tid);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        open = true;
        rewind();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td =  file.getTupleDesc();
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        Iterator<TDItem> iter = td.iterator();
        while(iter.hasNext()){
            TDItem item = iter.next();
            types.add(item.fieldType);
            names.add(tableAlias + "." +item.fieldName);
        }
        return new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return open && next != null; 
        
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple res = next;
        next = fIterator.hasNext() ? fIterator.next() : null;
        return res;
    }

    public void close() {
        open = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if(open){
            fIterator.open();
            next = fIterator.hasNext() ? fIterator.next() : null;
        }
    }
}
