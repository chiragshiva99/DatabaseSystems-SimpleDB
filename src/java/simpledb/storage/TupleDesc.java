package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int numOfFields;
    private TDItem[] tdItems;
    private HashMap<String, Integer> fieldMap;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        List<TDItem> ListOftdItem = Arrays.asList(tdItems);
        // some code goes here
        return (Iterator<TDItem>) ListOftdItem.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("There must be at least one entry!");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and FieldAr must have equal lengths");
        }

        this.numOfFields = typeAr.length;
        this.tdItems = new TDItem[numOfFields];
        this.fieldMap = new HashMap<>();

        for (int i = 0; i < numOfFields; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
            fieldMap.put(fieldAr[i], i); // inset into HashMap for lookup
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */

    // constructor chaining from line 96 to 109 --> check once if correct
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    private TupleDesc(TDItem[] array) {
        this.tdItems = array;
        this.numOfFields = array.length;
        this.fieldMap = new HashMap<>();
        for (int i = 0; i < numOfFields; i++) {
            fieldMap.put(array[i].fieldName, i);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        // return this.tdItems.length;
        return this.numOfFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (!validIndex(i)) {

            throw new NoSuchElementException("Must be a valid index");
        }

        return tdItems[i].fieldName;
    }

    private boolean validIndex(int i) {

        if (i >= 0 && i < numOfFields) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (!validIndex(i)) {

            throw new NoSuchElementException("Must be a valid index");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (!fieldMap.containsKey(name)) {

            throw new NoSuchElementException("No field with a matching name is found");
        }

        return fieldMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int s = 0;

        for (TDItem tdi_item : tdItems) {
            s += tdi_item.fieldType.getLen();
        }
        return s;
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
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

        // some code goes here
        TDItem[] one = td1.tdItems;
        TDItem[] two = td2.tdItems;

        int s1 = one.length;
        int s2 = two.length;

        int total = s1 + s2;
        TDItem[] tgt = new TDItem[total];

        System.arraycopy(one, 0, tgt, 0, s1);
        System.arraycopy(two, 0, tgt, s1, s2);
        // for (int i =0 ; i<size1; i++){
        // merged[i] = first[i];
        // }
        // for (int i =size1 ; i<totalSize; i++){
        // merged[i] = second[i-size1];
        // }
        return new TupleDesc(tgt);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {

        if (o == this || (o == null && this == null)) {
            return true;

        }

        else if ((o == null && this != null) || !(o instanceof TupleDesc) || (o != null && this == null)) {
            return false;
        }

        TupleDesc obj = (TupleDesc) o;
        if (this.numFields() == obj.numOfFields) {
            for (int i = 0; i < numOfFields; i++) {

                if (tdItems[i].fieldType.equals(obj.tdItems[i].fieldType)) {
                    return true;
                }
            }
        }

        return false;
    }
    // some code goes here

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String empty = "";

        for (int i = 0; i < tdItems.length - 1; i++) {

            empty += tdItems[i].fieldType + "(" + tdItems[i].fieldName + "), ";
        }

        empty += tdItems[tdItems.length - 1].fieldType + "(" + tdItems[tdItems.length - 1].fieldName + ")";

        return empty;
    }
}
