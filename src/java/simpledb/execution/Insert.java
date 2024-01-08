package simpledb.execution;

import java.io.IOException;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.IntField;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child; 
    private Integer tableId;
    private boolean tracker;
    private TupleDesc tupdesc = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid=t;
                this.child=child;
                this.tableId=tableId;
                this.tracker=false;
                TupleDesc tempTupdesc= Database.getCatalog().getTupleDesc(tableId);
                if(!(child.getTupleDesc().equals(tempTupdesc))){
                    throw new DbException("TupleDesc of child different from inserted value in table.");
                }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupdesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.tracker=false;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.open();
        this.close();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.tracker==false){
            this.tracker=true;
          int count = 0;
          while (child.hasNext()) {
            
            try {
              Database.getBufferPool().insertTuple(this.tid, this.tableId, child.next());
              count++;
            } catch (IOException e) {
              throw new DbException("Heapfile not found");
            }
          }
          Tuple inserted = new Tuple(this.tupdesc);
          inserted.setField(0, new IntField(count));
          return inserted;
        }
        else{
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[]Opiterator=new OpIterator[] {this.child};
        return Opiterator;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
