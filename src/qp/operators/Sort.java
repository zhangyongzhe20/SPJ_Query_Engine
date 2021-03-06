/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.TupleReader;

import java.util.ArrayList;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort
    private ArrayList<Attribute> attributeArrayList;
    protected int numBuff;
    protected int batchSize;
    protected String fileName;
    protected TupleReader tupleReader;
    protected int[] attrIndex;


    public Sort(Operator source, ArrayList<Attribute> attrList, String fileName) {
        super(OpType.SORT);

        this.base = source;
        this.attributeArrayList = attrList;
        //this.numBuff = numBuff;
        this.fileName = fileName;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Open file prepare a stream pointer to read input file
     */
    public boolean open() {
        System.out.println("Called open");
        return true;
//        if (!base.open()) {
//            return false;
//        }
//
//        //Create and prepare tuple reader
//        int tupleSize = base.getSchema().getTupleSize();
//        batchSize = Batch.getPageSize() / tupleSize;
//
//        tupleReader = new TupleReader(fileName, batchSize);
//
//        //Get list of attributes(column index) to sort on
//        Schema baseSchema = base.getSchema();
//        attrIndex = new int[attributeArrayList.size()];
//        for (int i = 0; i < attributeArrayList.size(); i++) {
//            Attribute attr = attributeArrayList.get(i);
//            int index = baseSchema.indexOf(attr);
//            attrIndex[i] = index;
//        }
//
//        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        return null;
    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        return false;
    }

    public Object clone() {
        return null;
    }

}
