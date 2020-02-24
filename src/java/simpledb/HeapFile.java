package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private Integer fileId; // table id
    private File file; // this file on disk
    private RandomAccessFile accessFile; // random access
    private TupleDesc td; // tuple desc of this table
    // private int pageNum; // the number of pages of the table 
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        // pageNum = (int) f.length() / BufferPool.getPageSize();
        try {
            accessFile = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you
     * will need to generate this tableid somewhere to ensure that each HeapFile has
     * a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying
     * the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        if (fileId == null)
            fileId = file.getAbsoluteFile().hashCode();
        return fileId.intValue();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pid.getPageNumber();
        byte[] data = new byte[pageSize];
        try {
            accessFile.seek(offset);
            int readBytes = accessFile.read(data);
            if (readBytes == pageSize) {
                HeapPageId hpid = (HeapPageId) pid;
                return new HeapPage(hpid, data);
            } else {
                throw new IOException("read a whole page data failed");
            }
        } catch (IOException e) {
            System.err.println("cannot read page");
            e.printStackTrace();
            return null;
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * page.getId().getPageNumber();
        accessFile.seek(offset);
        accessFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // System.err.println((int)Math.floor(file.length() * 1.0 / BufferPool.getPageSize()));
        return (int)Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>();
        int pageNum = this.numPages();
        for (int i = 0; i < pageNum; i++) {
            PageId pid = new HeapPageId(getId(), i);
            // System.err.println("get page");
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }
        }
        HeapPageId pid = new HeapPageId(getId(),pageNum);
        pageNum ++;
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];
        HeapPage page = new HeapPage(pid, data);
        page.insertTuple(t);
        writePage(page);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        page.deleteTuple(t);

        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        int pageNum = numPages();
        return new HeapFileIterator(this.getId(), pageNum, tid);
    }

}
