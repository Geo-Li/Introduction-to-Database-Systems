package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int groupbyField;
    private Type groupbyFieldType;
    private int aggregateField;
    private Op operator;
    private HashMap<Field, Integer> gbAggMap = new HashMap<Field, Integer>();
    private int no_grpCount = 0;
    
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	groupbyField = gbfield;
    	groupbyFieldType = gbfieldtype;
    	aggregateField = afield;
    	operator = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	// we don't really need this aggregate value for count operator
    	String aggVal = ((StringField) tup.getField(aggregateField)).getValue();
    	if (groupbyField == NO_GROUPING) {
    		no_grpCount += 1;
    	} else {
    		Field gbField = tup.getField(groupbyField);
    		if (!gbAggMap.containsKey(gbField)) {
    			gbAggMap.put(gbField, 0);
    		}
    		gbAggMap.put(gbField, gbAggMap.get(gbField)+1);
    	}    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	TupleDesc td;
    	if (groupbyField == NO_GROUPING) {
    		td = new TupleDesc(new Type[] {Type.INT_TYPE});
    		Tuple tuple = new Tuple(td);
    		switch (operator) {
    		case COUNT:
    			tuple.setField(0, new IntField(no_grpCount));
    			tuples.add(tuple);
    			break;
    		}
    	} else {
    		td = new TupleDesc(new Type[] {groupbyFieldType, Type.INT_TYPE});
    		for (Field gbField : gbAggMap.keySet()) {
    			Tuple tuple = new Tuple(td);
    			tuple.setField(0, gbField);
    			switch (operator) {
        		case COUNT:
        			tuple.setField(1, new IntField(gbAggMap.get(gbField)));
        			tuples.add(tuple);
        			break;
        		}
    		}
    	}
    	return new TupleIterator(td, tuples);
    }

}
