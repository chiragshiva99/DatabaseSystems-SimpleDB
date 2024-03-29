package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import simpledb.common.Type;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */

    private OpIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator iterator;
    private TupleDesc tupleDesc;

    public Aggregate(OpIterator child, int aField, int gField, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aField = aField;
        this.gField = gField;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
//        return -
        return gField;
    }


    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
//        return null;
        return child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
//        return null;
        return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
//        return null;
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        Type type;
        if(gField == -1){
            type = null;
        } else{
            type = child.getTupleDesc().getFieldType(gField);
        }
        if(child.getTupleDesc().getFieldType(aField) == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gField,type,aField,aop);
        }else if(child.getTupleDesc().getFieldType(aField) == Type.STRING_TYPE){
            aggregator = new StringAggregator(gField,type,aField,aop);
        }else{
            throw new DbException("This Iterator type isn't supported");
        }
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;
        if(iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(aField))" where aop and aField are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        return null;
        if(gField==-1){
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(aField)},
                    new String[]{aggregateFieldName()});
        }else{
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(gField),
                    child.getTupleDesc().getFieldType(aField)},
                    new String[]{groupFieldName(),aggregateFieldName()});
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        iterator.close();
        aggregator = null;
        iterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
