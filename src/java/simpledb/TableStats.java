package simpledb;

import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(final String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(final String tablename, final TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(final HashMap<String, TableStats> s) {
        try {
            final java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (final NoSuchFieldException e) {
            e.printStackTrace();
        } catch (final SecurityException e) {
            e.printStackTrace();
        } catch (final IllegalArgumentException e) {
            e.printStackTrace();
        } catch (final IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        final Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            final int tableid = tableIt.next();
            final TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int cardinality = 0;
    private int numPages = 0;
    private final Map<Integer, IntHistogram> intHistograms;
    private final Map<Integer, StringHistogram> stringHistograms;
    private final int ioCostPerPage;
    /**
     * Create a new TableStats object, that keeps track of statistics on each column
     * of a table
     * 
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate
     *                      between sequential-scan IO and disk seeks.
     */
    public TableStats(final int tableid, final int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        intHistograms = new HashMap<>();
        stringHistograms = new HashMap<>();
        final Map<Integer, Integer> mins = new HashMap<>(), maxs = new HashMap<>();

        final DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        final TupleDesc td = file.getTupleDesc();
        for (int i = 0, fieldNum = td.numFields(); i < fieldNum; i++) {
            // System.err.println("i:"+i);
            if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                mins.put(i, Integer.MAX_VALUE);
                maxs.put(i, Integer.MIN_VALUE);
            } else {
                stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        final DbFileIterator iter = file.iterator(new TransactionId());
        try {
            iter.open();
            try{
                while(iter.hasNext()){
                    Tuple tuple = iter.next();
                    for (int i = 0, fieldNum = td.numFields(); i < fieldNum; i++) {
                        if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                            IntField intField = (IntField) tuple.getField(i);
                            if(intField.getValue() < mins.get(i)){
                                mins.put(i, intField.getValue());
                            }
                            if(intField.getValue() > maxs.get(i)){
                                maxs.put(i, intField.getValue());
                            }
                        }else {
                            // stringhistogram do nothing
                        }
                    }
                    cardinality ++;
                }
            }finally{
                iter.close();
            }

            for(Integer index : mins.keySet()){
                if(mins.get(index) < maxs.get(index)){
                    intHistograms.put(index, new IntHistogram(NUM_HIST_BINS, mins.get(index), maxs.get(index)));
                }else {
                    intHistograms.put(index, new IntHistogram(NUM_HIST_BINS, Integer.MIN_VALUE, Integer.MAX_VALUE)); // combine
                }
            }
            iter.open();
            try{
                while(iter.hasNext()){
                    Tuple tuple = iter.next();
                    for(Integer index : intHistograms.keySet()){
                        // System.err.println("addvalue");
                        intHistograms.get(index).addValue(((IntField)tuple.getField(index)).getValue());
                    }
                    for(Integer index : stringHistograms.keySet()){
                        stringHistograms.get(index).addValue(((StringField)tuple.getField(index)).getValue());
                    }
                        
                }
            }finally{
                iter.close();
            }
        } catch (DbException | TransactionAbortedException e) {
           throw new RuntimeException(e);
        }

        int pageSize = BufferPool.getPageSize();
        this.numPages = (cardinality * td.getSize()+ pageSize - 1) / pageSize;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost to
     * read a page is costPerPageIO. You can assume that there are no seeks and that
     * no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so if
     * the last page of the table only has one tuple on it, it's just as expensive
     * to read as a full page. (Most real hard drives can't efficiently address
     * regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(final double selectivityFactor) {
        return (int) (selectivityFactor * cardinality);
    }

    /**
     * The average selectivity of the field under op.
     * 
     * @param field the index of the field
     * @param op    the operator in the predicate The semantic of the method is
     *              that, given the table, and then given a tuple, of which we do
     *              not know the value of the field, return the expected
     *              selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(final int field, final Predicate.Op op) {
        if(intHistograms.containsKey(field)){
            return intHistograms.get(field).avgSelectivity();
        }else {
            return stringHistograms.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(final int field, final Predicate.Op op, final Field constant) {
        
        if(intHistograms.containsKey(field)){
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }else {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return cardinality;
    }

}
