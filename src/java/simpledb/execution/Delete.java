package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId tid;
    private OpIterator child;
    private boolean bool;
    private final TupleDesc tuple_disc = new TupleDesc(new Type[]{Type.INT_TYPE});

    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid=t;
        this.child=child;
        this.bool=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
//        return null;
        return this.tuple_disc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.bool=false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;
        if(this.bool==false){
            this.bool=true;
            int count = 0;

            while (child.hasNext())
                try {
                    Database.getBufferPool().deleteTuple(tid, child.next());
                    ++count;
                } catch (IOException e) {
                    throw new DbException("Unable to file heapfile");
                }
            Tuple deleted = new Tuple(tuple_disc);
            deleted.setField(0, new IntField(count));
            return deleted;
        }
        else{return null;}
    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
//        return null;
        OpIterator[]Opiterator=new OpIterator[] {this.child};
        return Opiterator;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
