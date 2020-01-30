package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final Field EMPTY_FIELD = new IntField(0);
    private final int gbfield, afield; // group by field, aggregate field.
    // private final Type gbfieldtype; // group by value
    private final TupleDesc tupleDesc;
    private final Op operator;
    private Map<Field, Integer> aggregateValues;
    private Map<Field, Integer> groupCount;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        // this.gbfieldtype = gbfieldtype;
        this.operator = what;
        this.tupleDesc = gbfieldtype == null ? new TupleDesc(new Type[] { Type.INT_TYPE })
                : new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        this.aggregateValues = new HashMap<>();
        this.groupCount = new HashMap<>();
    }

    private Integer getPrevValue(Field key) {
        Integer prevValue = aggregateValues.get(key);
        if (prevValue == null) {
            switch (operator) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case AVG:
            case COUNT:
            case SUM:
                return 0;
            default:
                throw new UnsupportedOperationException("operator not supported");
            }
        }
        return prevValue;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == NO_GROUPING ? EMPTY_FIELD : tup.getField(gbfield);
        Integer prevValue = getPrevValue(key);
        Integer groupNum = groupCount.containsKey(key) ?  groupCount.get(key) : 0;
        int thisValue = tup.getField(this.afield).hashCode();
        switch (operator) {
        case MIN:
            prevValue = Math.min(prevValue.intValue(), thisValue);
            break;
        case MAX:
            prevValue = Math.max(prevValue.intValue(), thisValue);
            break;
        case COUNT:
            prevValue = prevValue + 1;
            break;
        case AVG: // divied when creating iterator
        case SUM:
            prevValue = prevValue + thisValue;
            break;
        default:
            throw new UnsupportedOperationException("operator not supported");
        }
        aggregateValues.put(key, prevValue);
        groupCount.put(key, groupNum + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
     *         using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {

        return new OpIterator() {
            private static final long serialVersionUID = 1L;
            private Iterator<Field> child;
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                child = aggregateValues.keySet().iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                child = aggregateValues.keySet().iterator();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple tuple = new Tuple(tupleDesc);
                Field key = gbfield == NO_GROUPING ? EMPTY_FIELD : child.next();
                int value = operator == Op.AVG ? aggregateValues.get(key) / groupCount.get(key) : aggregateValues.get(key);
                Field agg = new IntField(value);
                if (gbfield == NO_GROUPING) {
                    tuple.setField(0,agg);
                    this.close();
                }else {
                    tuple.setField(0, key);
                    tuple.setField(1, agg);
                }
                return tuple;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return child != null && child.hasNext();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                child = null;
            }
        };
    }

}
