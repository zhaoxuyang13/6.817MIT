package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable { /*  TODO-2  */

    /* * 
     *  All the filed TDItems in this TupleDesc
     * */
    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(final Type t, final String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return items.iterator();
        // return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(final Type[] typeAr, final String[] fieldAr) {
        items = new ArrayList<TDItem>();
        if(typeAr.length != fieldAr.length) {
            System.err.println("type length mismatch with field length");
        }
        for(int i = 0; i < typeAr.length; i ++){
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(final Type[] typeAr) {
        // some code goes here
        items = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i ++){
            items.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(final int i) throws NoSuchElementException {
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(final int i) throws NoSuchElementException {
        return items.get(i).fieldType;
        // return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(final String name) throws NoSuchElementException {
        if(name == null){
            throw new NoSuchElementException("null is not a valid field name");
        }
        int size = items.size();
        for(int i = 0; i < size; i ++){
            if(name.equals(items.get(i).fieldName))
                return i;
        }
        throw new NoSuchElementException(name + " is not a valid field name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(TDItem item: items){
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(final TupleDesc td1, final TupleDesc td2) {
        Iterator<TDItem> iter1 = td1.iterator(), iter2 = td2.iterator();
        List<Type> types = new ArrayList<>();
        List<String> strings =  new ArrayList<>();
        while(iter1.hasNext()){
            TDItem item = iter1.next();
            types.add(item.fieldType);
            strings.add(item.fieldName);
        }
        while(iter2.hasNext()){
            TDItem item = iter2.next();
            types.add(item.fieldType);
            strings.add(item.fieldName);
        }
        return new TupleDesc(types.toArray(new Type[0]), strings.toArray(new String[0]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(final Object o) {
        if(o == null || o.getClass() != TupleDesc.class)
            return false;
        TupleDesc ot = (TupleDesc) o;
        int size = items.size();
        if(ot.numFields() != size){
            return false;
        }
        for(int i = 0; i <  size; i ++){
            if(ot.getFieldType(i) != items.get(i).fieldType){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int hash = 0;
        Iterator<TDItem> iter = items.iterator();
        while(iterator().hasNext()){
            hash = Objects.hash(iter.next().fieldType, hash);
        }
        return hash;
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        int size = items.size();
        for(int i = 0; i < size; i ++){
            buffer.append(
                items.get(i).fieldType + "(" + items.get(i).fieldName   + ")"
                );
            if(i != size - 1)
                buffer.append(",");
        }
        return buffer.toString();
    }
}
