package simpledb.common;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

  private HashMap<PageId, PageLock> pgLock;
  public HashMap<TransactionId, HashSet<PageId>> transacMap;
  private HashMap<TransactionId,HashSet<TransactionId>> waitMap;


  public LockManager() {
    pgLock = new HashMap<>();
    transacMap= new HashMap<>();
    waitMap = new HashMap<>();
  }

  synchronized public boolean holdsLock(TransactionId tid, PageId pid) {
    if (!pgLock .containsKey(pid)) {
      return false;
    }
    PageLock lockPage = pgLock .get(pid);
    return lockPage.holdsLock(tid);
  }

  synchronized public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws DbException, TransactionAbortedException, InterruptedException {
    pgLock .putIfAbsent(pid, new PageLock());
    transacMap.putIfAbsent(tid, new HashSet<>());


    HashSet<PageId> currentPages = transacMap.get(tid);
    currentPages.add(pid);
    transacMap.put(tid, currentPages);

    PageLock page = pgLock.get(pid);

    while(true){
      if(page.holdsWriteLock()){
        if(perm.equals(Permissions.READ_WRITE)){
          if(page.holdsWriteLock(tid)) return;
        }else if(perm.equals(Permissions.READ_ONLY)){
          if(page.getWriteLock().equals(tid)) return;
        }else{
          throw new DbException("permission does not exist");
        }
        waitMap.putIfAbsent(tid,new HashSet<>());
        if(!waitMap.get(tid).contains(page.getWriteLock()))
        waitMap.get(tid).add(page.getWriteLock());
        if(isDeadLock()){
          waitMap.get(tid).remove(page.getWriteLock());
          notifyAll();
          throw new TransactionAbortedException();
        }
      }else{
        if(perm.equals(Permissions.READ_ONLY)){
          page.addReadLock(tid);
          return;
        }else if(perm.equals(Permissions.READ_WRITE)){
          if(page.getReadLocks().isEmpty()){
            page.setWriteLock(tid);
            return;
          }
          if((page.getReadLocks().size()==1) && (page.getReadLocks().contains(tid))){
            page.upgradeLock(tid);
            return;
          }else{
            waitMap.putIfAbsent(tid, new HashSet<>());
            HashSet<TransactionId> tids = waitMap.get(tid);
            for (TransactionId readLock : page.getReadLocks()) {
              if (!readLock.equals(tid)) {
                if (!tids.contains(readLock)) {
                  tids.add(readLock);
                }
              }
            }
            waitMap.replace(tid, tids);
            if (isDeadLock()) {
              for (TransactionId readLock2 : page.getReadLocks()) {
                if (waitMap.get(tid).contains(readLock2)) {
                    waitMap.get(tid).remove(readLock2);
                }
              }
              notifyAll();
              throw new TransactionAbortedException();
            }
          }
        }else{
          throw new DbException("permission does not exist");
        }
      }
      wait();
    }
  }


  synchronized public void releaseLock(TransactionId tid, PageId pid) throws DbException {
    if (pgLock.containsKey(pid)) {
        pgLock.get(pid).releaseLock(tid);
        transacMap.get(tid).remove(pid);
      notifyAll();
    }
  }


  synchronized public void releaseAllLocks(TransactionId tid) throws DbException {
    HashSet<PageId> currPages = transacMap.get(tid);
    if (currPages == null) return;

    Iterator<PageId> page_iter = currPages.iterator();
    while (page_iter.hasNext()) {
      PageId pid = page_iter.next();
      pgLock.get(pid).releaseLock(tid);
    }
    transacMap.remove(tid);
    if(waitMap.containsKey(tid)) waitMap.remove(tid);

    notifyAll();
  }

  synchronized public boolean isDeadLock(){
    HashMap<TransactionId,HashSet<TransactionId>> being_waited = new HashMap<>(); 
    HashMap<TransactionId,Integer> waiting_count = new HashMap<>(); 
    Deque<TransactionId> toExecute = new LinkedList<>(); 

    for(TransactionId tid1:transacMap.keySet()){
      waiting_count.putIfAbsent(tid1,0);
      if(waitMap.keySet().contains(tid1)){
        int temp_count = 0;
        for(TransactionId tid2:waitMap.get(tid1)){
          being_waited.putIfAbsent(tid2,new HashSet<>());
          if(!being_waited.get(tid2).contains(tid1)){
            HashSet<TransactionId> tids = being_waited.get(tid2);
            tids.add(tid1);
            being_waited.replace(tid2,tids);
          }
          if(transacMap.containsKey(tid2)) temp_count++;
        }
        waiting_count.replace(tid1,temp_count);
      }
    }

    for(TransactionId tid:waiting_count.keySet()){
      if(waiting_count.get(tid)==0) toExecute.offer(tid);
    }

    int count = 0;
    while (!toExecute.isEmpty()) {
      TransactionId tid3 = toExecute.poll();
      if (being_waited.containsKey(tid3)) {
        for (TransactionId tid4 : being_waited.get(tid3)) {
          waiting_count.replace(tid4,waiting_count.get(tid4)-1);
          if(waiting_count.get(tid4)==0) toExecute.offer(tid4);
        }
      }
      count++;
    }

    return (count!=transacMap.size());
  }

}