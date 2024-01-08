package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private HashMap<Field, Integer> gbMapping;
    private int cnt = -1;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT){
            throw new IllegalArgumentException("only COUNT supported");
        }
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.gbMapping = new HashMap<>();
        if(this.gbfield == Aggregator.NO_GROUPING || this.gbfieldType == null){
            cnt = 0;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(cnt>-1){
            cnt++;
            return;
        }

        Field fieldGrouping = tup.getField(this.gbfield);
        int x = gbMapping.getOrDefault(fieldGrouping,0);
        gbMapping.put(fieldGrouping,++x);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new OpIterator() {
            private TupleDesc tupdesc;
            private ArrayList<Tuple> tupleLs;
            private Iterator<Tuple> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                tupleLs = new ArrayList<>();
                if(cnt>-1){
                    tupdesc = new TupleDesc(new Type[] {Type.INT_TYPE});
                    Tuple tup = new Tuple(tupdesc);
                    tup.setField(0, new IntField(cnt));
                    tupleLs.add(tup);
                }
                else{
                    tupdesc = new TupleDesc(new Type[] {gbfieldType,Type.INT_TYPE});
                    for(Map.Entry<Field,Integer> entry: gbMapping.entrySet()){
                        Tuple tup = new Tuple(tupdesc);
                        tup.setField(0, entry.getKey());
                        tup.setField(1,new IntField(entry.getValue()));
                        tupleLs.add(tup);
                    }
                }
                iterator = tupleLs.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupdesc;
            }

            @Override
            public void close() {
                tupdesc = null;
                tupleLs = null;
                iterator = null;
            }
        };
    }

}
