package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;
    private int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableid = f.getAbsoluteFile().hashCode();
        Database.getCatalog().addTable(this);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableid;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getPageNumber() > numPages()) {
            throw new IllegalArgumentException("This page files doesnt exist");
        }
        try {
            if (pid.getPageNumber()==numPages()){
                HeapPage page = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
                writePage(page);
                return page;
            }

            int offset = pid.getPageNumber()*BufferPool.getPageSize();
            byte[] data = Files.readAllBytes(f.toPath());
            data = Arrays.copyOfRange(data,offset,offset+BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("This page files doesnt exist");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPage p = (HeapPage)page;
        RandomAccessFile file = new RandomAccessFile(f,"rw");
        file.seek(BufferPool.getPageSize()*page.getId().getPageNumber());
        file.write(p.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.f.length()+BufferPool.getPageSize()-1)/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pages = new ArrayList<>();
        for(int i = 0; i<numPages(); i++){
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(tableid,i),Permissions.READ_WRITE);
            if(page.getNumEmptySlots()>0) {
                page.insertTuple(t);
                pages.add(page);
                return pages;
            } else {
                Database.getBufferPool().unsafeReleasePage(tid,page.pid);
            }
        }
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(tableid,numPages()),Permissions.READ_WRITE);
        page.insertTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        RecordId rid = t.getRecordId();
        if(rid==null) throw new DbException("Delete tuple failed: tuple slot has already been deleted");
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid,rid.getPageId(),Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private ArrayList<HeapPageId> pageId;
            private Iterator<Tuple> tupleIterator;
            private int pageNo = -1;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageId =  new ArrayList<>();
                for(int i=0;i<numPages();i++){
                    pageId.add(new HeapPageId(getId(),i));
                }
                while((tupleIterator==null)&&(pageNo<numPages()-1)){
                    pageNo += 1;
                    HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid,pageId.get(pageNo),Permissions.READ_ONLY);
                    if(page!=null){
                        tupleIterator = page.iterator();
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tupleIterator==null){
                    return false;
                }
                if(tupleIterator.hasNext()){
                    return true;
                }
                int temp_pageNo = pageNo;
                while((!tupleIterator.hasNext())&&(temp_pageNo<numPages()-1)){
                    temp_pageNo += 1;
                    HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid,pageId.get(temp_pageNo),Permissions.READ_ONLY);
                    if(page!=null){
                        Iterator<Tuple> iterator = page.iterator();
                        if(iterator.hasNext()){
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(tupleIterator==null){
                    throw new NoSuchElementException("no such element (need to open first)");
                }
                if(tupleIterator.hasNext()){
                    return tupleIterator.next();
                }
                else if(pageNo<numPages()-1){
                    while((!tupleIterator.hasNext())&&(pageNo<numPages()-1)) {
                        pageNo += 1;
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId.get(pageNo), Permissions.READ_ONLY);
                        if (page != null) {
                            tupleIterator = page.iterator();
                            if(tupleIterator.hasNext()){
                                return tupleIterator.next();
                            }
                        }
                    }
                }
                throw new NoSuchElementException("there are no more tuples");
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public void close() {
                tupleIterator = null;
                pageId = null;
                pageNo = -1;
            }
        };
    }

}
