package simpledb.common;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.HashSet;

public class PageLock {

    private TransactionId lockWrite; 
    private HashSet<TransactionId> lockRead;


    public PageLock() {
        lockWrite = null;
        lockRead = new HashSet<>();
    }

    public TransactionId getWriteLock() {
        return lockWrite;
    }

    public void setWriteLock(TransactionId lockWrite) {
        this.lockWrite  = lockWrite;
    }

    public HashSet<TransactionId> getReadLocks() {
        return lockRead;
    }

    public void addReadLock(TransactionId readLock) {
        if(!lockRead.contains(readLock)) lockRead.add(readLock);
    }


    void releaseWriteLock(TransactionId tid) {
        if(lockWrite==tid){
            lockWrite = null;
        }

    }

    void releaseReadLock(TransactionId tid) {
        if(lockRead.contains(tid)) lockRead.remove(tid);
    }

    void releaseLock(TransactionId tid) {
        releaseWriteLock(tid);
        releaseReadLock(tid);
    }

    boolean holdsWriteLock() {
        if (lockWrite == null)
            return false;
        return true;
    }

    boolean holdsReadLock() {
        if(lockRead.size()>0) return true;
        return false;
    }

    boolean holdsWriteLock(TransactionId tid) {
        if (lockWrite == null) return false;
        return lockWrite.equals(tid);
    }

    boolean holdsReadLock(TransactionId tid) {
        return lockRead.contains(tid);
    }

    boolean holdsLock(TransactionId tid) {
        return holdsReadLock(tid) || holdsWriteLock(tid);
    }

    void upgradeLock(TransactionId tid) {
        lockWrite = tid;
        lockRead = new HashSet<>();
    }
}
