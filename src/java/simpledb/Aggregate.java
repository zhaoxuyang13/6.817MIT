package simpledb;

import java.security.Principal;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private Aggregator agg;
    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op operator;
    private OpIterator iterator;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.operator = aop;
        Type atype = child.getTupleDesc().getFieldType(afield);
        Type gtype = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        if (atype == Type.INT_TYPE) {
            this.agg = new IntegerAggregator(gfield, gtype, afield, aop);
        } else if (atype == Type.STRING_TYPE) {
            this.agg = new StringAggregator(gfield, gtype, afield, aop);
        } else {
            this.agg = null;
        }
        try {
            child.open();
            while (child.hasNext()){
                agg.mergeTupleIntoGroup(child.next()); 
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        this.iterator = agg.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        rewind();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // System.err.println(iterator.next().toString());
        return iterator.hasNext() ?  iterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // System.err.println("-------------rewind");
        iterator = agg.iterator();
        iterator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void close() {
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
        Type atype = child.getTupleDesc().getFieldType(afield);
        Type gtype = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        if (atype == Type.INT_TYPE) {
            this.agg = new IntegerAggregator(gfield, gtype, afield, operator);
        } else if (atype == Type.STRING_TYPE) {
            this.agg = new StringAggregator(gfield, gtype, afield, operator);
        } else {
            this.agg = null;
        }
        try {
            child.open();
            while (child.hasNext()){
                agg.mergeTupleIntoGroup(child.next()); 
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        this.iterator = agg.iterator();
    }
    
}
