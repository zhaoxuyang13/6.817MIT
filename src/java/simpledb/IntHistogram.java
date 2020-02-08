package simpledb;

import java.util.ArrayList;
import java.util.List;



/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    private final int[] buckets;
    private final int min, max;
    private final double width ;
    private int ntups ;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[Math.min(buckets, max-min+1)];
        this.min = min;
        this.max = max + 1;
        this.width = (1.0 + max - min ) / this.buckets.length;
        this.ntups = 0;
    }
    private int catagory(int v){
        if(v < min) return 0;
        if(v > max) return buckets.length - 1;
        return (int)((v-min)/width);
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // System.err.println(v + " is " + catagory(v));
        buckets[catagory(v)] += 1;
        ntups += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = catagory(v);
        // System.err.println(min + " " + max + " " + v + " " + index);
        double sel = 0;
        switch (op) {
            case EQUALS:
                return buckets[index] / width / ntups;
            case NOT_EQUALS:
                return 1 - buckets[index] / width / ntups;
            case LESS_THAN:
                for(int i = 0; i < index; i ++){
                    sel += buckets[i];
                }
                sel += buckets[index] / width * (v - index * width - min);
                return sel / ntups;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v -1);
            default:
                break;
        }
    	return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        for(int i = 0 ;i < buckets.length; i ++){
            buffer.append(", "+ i +": " + buckets[i]);
        }
        return buffer.toString();
    }
}
