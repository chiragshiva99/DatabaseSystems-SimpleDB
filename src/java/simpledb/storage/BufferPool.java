package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.RuntimeErrorException;

import simpledb.common.LockManager;
import java.util.HashSet;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    private int max_pages;
    final ConcurrentHashMap<PageId, Page> store_cache;
    LockManager lockmanager;
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.max_pages = numPages;
        this.store_cache = new ConcurrentHashMap<PageId, Page>();
        lockmanager = new LockManager();
        // some code goes here

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
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            this.lockmanager.acquireLock(tid, pid, perm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            this.lockmanager.acquireLock(tid, pid, perm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (store_cache.containsKey(pid)) {
            return store_cache.get(pid);
        } else {

            if (store_cache.size() >= max_pages) {
                evictPage();
            }

            DbFile DatabaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page P1 = DatabaseFile.readPage(pid);
            store_cache.put(pid, P1);
            return P1;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            this.lockmanager.releaseLock(tid, pid);
        } catch (DbException ex) {

            throw new RuntimeException("unsafe release page runtime");
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2

        return this.lockmanager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> restore = this.lockmanager.transacMap.get(tid);
        if (restore == null) {

            return;
        }
        try {
            if (commit) {
                flushPages(tid);
            } else {
                for (PageId pid : restore) {

                    discardPage(pid);
                }
            }
            this.lockmanager.releaseAllLocks(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> ls_page = (ArrayList<Page>) file.insertTuple(tid, t);
        for (Page pg : ls_page) {
            pg.markDirty(true, tid);
            if (this.store_cache.size() > max_pages)
                evictPage();
            this.store_cache.put(pg.getId(), pg);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId() == null) {
            throw new DbException("tuple already deleted");
        }

        List<Page> ls2_page = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId())
                .deleteTuple(tid, t);
        for (Page pg : ls2_page) {
            pg.markDirty(true, tid);
            this.store_cache.put(pg.getId(), pg);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : this.store_cache.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        store_cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        if (pid != null) {
            if (!this.store_cache.containsKey(pid)) {
                return;
            }
            Page pg = this.store_cache.get(pid);
            if (!(pg.isDirty() == null)) {

                pg.markDirty(false, null);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);

            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> pgid = this.lockmanager.transacMap.get(tid);
        for (PageId pageId : pgid) {
            if (store_cache.containsKey(pageId)) {

                Page pg = store_cache.get(pageId);

                if (pg.isDirty() != null) {
                    flushPage(pageId);
                    pg.setBeforeImage();

                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (store_cache.keySet().size() == 0) {

            throw new DbException("no pages to evict");
        }
        PageId pgid = null;
        boolean cl = false;
        Iterator<PageId> pg_iter = store_cache.keySet().stream().iterator();

        while (pg_iter.hasNext()) {

            PageId cur_pgid = pg_iter.next();

            if (store_cache.get(cur_pgid).isDirty() == null) {

                pgid = cur_pgid;
                cl = true;
                break;
            }
        }
        if (cl) {

            try {
                flushPage(pgid);
            } catch (IOException e) {
                e.printStackTrace();
            }

            store_cache.remove(pgid);

        } else {

            throw new DbException("No clean page to evict");
        }

    }

}