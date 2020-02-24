package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.LinkedHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private static final class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private static final long serialVersionUID = 1L;
        private int cacheSize;
        private Map.Entry<K, V> eldest;

        public LRUCache(int cacheSize) {
            super(16, (float) 0.75, true);
            this.cacheSize = cacheSize;
            this.eldest = null;
        }

        public Map.Entry<K, V> eldestEntry() {
            return this.eldest;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            this.eldest = eldest;
            return size() > cacheSize;
        }
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final LRUCache<PageId, Page> id2Page;
    private final Map<PageId, Integer> id2Index;
    private final Set<Integer> indexPool;
    private final LockManager lockManager;
    Integer numPages;
    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        id2Page = new LRUCache<>(numPages);
        this.numPages = numPages;
        this.lockManager = new LockManager(numPages);
        this.id2Index = new HashMap<>();
        this.indexPool = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            indexPool.add(i);
        }
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, a page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        Catalog catalog = Database.getCatalog();
        System.err.println(tid.toString() + " get " + pid.toString() + " start");
        synchronized (id2Page) {
            if (id2Page.containsKey(pid)) {
                try {
                    System.err.println(tid.toString() + " get " + pid.toString() + " wait for lock");
                    lockManager.acquire(tid, id2Index.get(pid), perm);
                } catch (InterruptedException e) {
                    System.err.println(tid.toString() + " interuppted");
                    e.printStackTrace();
                }
                System.err.println(tid.toString() + " get " + pid.toString() + " gotten");
                return id2Page.get(pid);
            } else {
                System.err.println(tid.toString() + " get " + pid.toString() + " new buffer page");
                Page page = catalog.getDatabaseFile(pid.getTableId()).readPage(pid);
                if (id2Page.size() <= numPages) {
                    if (id2Page.size() == numPages) {
                        evictPage();
                    }
                    if (!indexPool.isEmpty()) {
                        Integer index = indexPool.iterator().next();
                        System.err.println(tid.toString() + " get " + pid.toString() + " new index get " + index);
                        indexPool.remove(index);
                        if (id2Index.containsKey(pid) && !id2Index.get(pid).equals(index)) {
                            System.err.println("pid already has mismatched index");
                        }
                        id2Index.put(pid, index);
                    } else {
                        System.err.println("error,index pool shouldnt be empty");
                    }
                    try {
                        lockManager.acquire(tid, id2Index.get(pid), perm);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    id2Page.put(pid, page);
                } else {
                    throw new DbException("buffer pool is full");
                }
                return page;
            }
        }
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        if (!id2Index.containsKey(pid)) {
            throw new IllegalArgumentException("page not in buffer");
        }
        lockManager.release(tid, id2Index.get(pid));

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.isHolding(tid, id2Index.get(p));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // System.err.println("---------------");
        // System.err.println(commit);
        if (commit) {
            flushPages(tid);
        }
        for (Entry<PageId, Page> entry : id2Page.entrySet()) {
            Integer index = id2Index.get(entry.getKey());
            if (lockManager.isHolding(tid, index)) {
                // System.err.println("release pid:" + entry.getKey().toString() +", tid is:" +
                // tid.toString() + " , is dirty " + entry.getValue().isDirty());
                if (!commit && tid.equals(entry.getValue().isDirty())) {
                    // System.err.println("discard");
                    discardPage(entry.getKey());
                }
                lockManager.release(tid, index);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        // System.err.println("insert");
        ArrayList<Page> pages = file.insertTuple(tid, t);
        for (Page page : pages) {
            // System.err.println("markdirty page: "+ page.getId().toString() + ",for tid: "
            // + tid);
            page.markDirty(true, tid);
            // System.err.println(page.isDirty());
            id2Page.put(page.getId(), page);
            // System.err.println(id2Page.get(page.getId()).isDirty());
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {

        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        for (Page page : pages) {
            // System.err.println("delete markdirty page: "+ page.getId().toString() + ",for
            // tid:" + tid);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Entry<PageId, Page> entry : id2Page.entrySet()) {
            flushPage(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        id2Page.remove(pid);
        Integer index = id2Index.get(pid);
        if (indexPool.contains(index)) {
            System.err.println("index still in the pool");
        }
        indexPool.add(index);
        id2Index.remove(pid);

    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = id2Page.get(pid);
        flushPage(pid, page);
    }

    private synchronized void flushPage(PageId pid, Page page) throws IOException {
        if (page == null) {
            System.err.println("no such pid to flush");
        }
        TransactionId tid = page.isDirty();
        if (null != tid) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, tid);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Entry<PageId, Page> entry : id2Page.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            // System.err.println("iterating :" +pid.toString());
            if (lockManager.isHolding(tid, id2Index.get(pid))) {
                flushPage(pid, page);
                page.setBeforeImage();
            }
        }
    }

    private synchronized PageId getEvictPage() throws DbException {
        PageId eldest = id2Page.eldestEntry().getKey();
        if (id2Page.get(eldest).isDirty() != null) {
            for (Entry<PageId, Page> entry : id2Page.entrySet()) {
                if (entry.getValue().isDirty() == null) {
                    return entry.getKey();
                }
            }
            throw new DbException("no clean page left");
        } else
            return eldest;
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId evictPage = getEvictPage();
        try {
            flushPage(evictPage);
        } catch (IOException e) {
            System.err.println("IO exception when flushing page");
            e.printStackTrace();
        }
        discardPage(evictPage);
    }

}
