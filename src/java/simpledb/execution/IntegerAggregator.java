package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private final int gbField;
    private final Type gbFieldType;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> gbHashMap;
    private HashMap<Field, Integer> gbAvgHashMap;
    private Field noGbKey;

    public IntegerAggregator(int gbField, Type gbFieldType, int afield, Op what) {
        // some code goes here
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.afield = afield;
        this.what = what;
        this.gbHashMap = new HashMap<Field,Integer>();
        this.gbAvgHashMap = new HashMap<Field,Integer>();
        this.noGbKey = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field curGbField;
        IntField curAggField;

        if (this.gbField == Aggregator.NO_GROUPING){
            curGbField = null;
        }
        else {
            curGbField = tup.getField(this.gbField);
        }
        curAggField = (IntField) tup.getField(this.afield);
        int curVal = curAggField.getValue();


        switch (this.what){

            case COUNT:
                if (!gbHashMap.containsKey(curGbField)){
                    gbHashMap.put(curGbField, 1);
                } else{
                    int curCountVal = gbHashMap.get(curGbField);
                    gbHashMap.put(curGbField, curCountVal + 1);
                }
                return;

            case SUM:
                if (!gbHashMap.containsKey(curGbField)){
                    gbHashMap.put(curGbField, curVal);
                } else{
                    int curSumVal = gbHashMap.get(curGbField);
                    gbHashMap.put(curGbField, curSumVal + curVal);
                }
                return;

            case AVG:
                if (!gbHashMap.containsKey(curGbField)){
                    gbHashMap.put(curGbField, 1);
                    gbAvgHashMap.put(curGbField, curVal);
                } else{
                    int curCount = gbHashMap.get(curGbField);
                    int curSum = gbAvgHashMap.get(curGbField);
                    gbHashMap.put(curGbField, curCount+1);
                    gbAvgHashMap.put(curGbField, curSum + curVal);
                }
                return;

            case MIN:
                if (!gbHashMap.containsKey(curGbField)){
                    gbHashMap.put(curGbField, curVal);
                } else{
                    int curMinVal = gbHashMap.get(curGbField);
                    gbHashMap.put(curGbField, Math.min(curMinVal,curVal));
                }
                return;

            case MAX:
                if (!gbHashMap.containsKey(curGbField)){
                    gbHashMap.put(curGbField, curVal);
                } else{
                    int curMaxVal = gbHashMap.get(curGbField);
                    gbHashMap.put(curGbField, Math.max(curMaxVal,curVal));
                }
                return;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private TupleDesc tupledesc;
            private Tuple[] aggregateVal;
            private int curIndex = 0;

            @Override
            public void open() throws DbException, TransactionAbortedException{

                // Single tuple with aggregateVal
                if (gbField == Aggregator.NO_GROUPING){
                    aggregateVal = new Tuple[1];
                    tupledesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple curTuple = new Tuple(tupledesc);
                    if (what != Op.AVG){
                        curTuple.setField(0, new IntField(gbHashMap.get(noGbKey)));
                    } else{
                        int avg = gbAvgHashMap.get(noGbKey)/gbHashMap.get(noGbKey);
                        curTuple.setField(0, new IntField(avg));
                    }
                    aggregateVal[0] = curTuple;
                }
                // Pair tuple with aggrVal
                else{
                    // Non-Average
                    if (what != Op.AVG){
                        aggregateVal = new Tuple[gbHashMap.size()];
                        tupledesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
                        int counter = 0;
                        for (Field gbField: gbHashMap.keySet()){
                            Tuple curTuple = new Tuple(tupledesc);
                            curTuple.setField(0, gbField);
                            curTuple.setField(1, new IntField(gbHashMap.get(gbField)));
                            aggregateVal[counter] = curTuple;
                            counter++;
                        }
                    }
                    // Average
                    else{
                        aggregateVal = new Tuple[gbAvgHashMap.size()];
                        tupledesc = new TupleDesc(new Type[] {gbFieldType, Type.INT_TYPE});
                        int counter = 0;
                        for (Field gbField: gbAvgHashMap.keySet()){
                            Tuple curTuple = new Tuple(tupledesc);
                            int avgVal = gbAvgHashMap.get(gbField)/gbHashMap.get(gbField);
                            curTuple.setField(0, gbField);
                            curTuple.setField(1, new IntField(avgVal));
                            aggregateVal[counter] = curTuple;
                            counter++;
                        }
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException{
                return curIndex < aggregateVal.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
                if (this.hasNext()){
                    Tuple tuple = aggregateVal[curIndex++];
                    return tuple;
                } else{
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException{
                this.close();
                this.open();
            }

            @Override
            public void close(){
                tupledesc = null;
                aggregateVal = null;
                curIndex = 0;
            }

            @Override
            public TupleDesc getTupleDesc(){
                return tupledesc;
            }
        };
    }

}