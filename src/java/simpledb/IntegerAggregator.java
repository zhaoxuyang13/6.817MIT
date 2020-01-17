package simpledb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield,afield;
    private final Type gbfieldtype;
    private final TupleDesc tupleDesc;
    private final Op operator;
    private Map<Field, Integer> aggregateValues;
    private Map<Field, Integer> groupCount;
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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.operator = what;
        this.tupleDesc = gbfieldtype == null ? new TupleDesc(new Type[]{Type.INT_TYPE}) 
                                            : new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
    }

    private Integer getPrevValue(Field key){
        r
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Integer prevValue = aggregateValues.get(key);
        Integer groupNum = groupCount.get(key);
        switch (operator) {
            case MIN:

                break;
            case MAX:
                break;
            case AVG:   
                break;
            case COUNT:
                break;
            case SUM:
                break;
            default:
                throw new
                UnsupportedOperationException("operator not supported");
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
        throw new
        UnsupportedOperationException("please implement me for lab2");
    }

}
