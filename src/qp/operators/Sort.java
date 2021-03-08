/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort
    int sortOn;
    int batchSize;
    int numBuff;

    public Sort(Operator base, boolean isAsc, boolean isDesc, int sortOn, int type, int numBuff) {
        super(type);
        this.base = base;
        this.sortOn = sortOn;
        this.numBuff = numBuff;
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
        System.out.println("Sort.Open() called");
        base.open();

        //Create and prepare tuple reader
        int tupleSize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        Schema baseSchema = base.getSchema();

        generateSortedRuns();


        System.out.println("Sort.Open() completed successfully");
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        return null;
    }



    /*Read in numBuff batches and add tuples to arraylist, sort arraylist, write to file*/
    public void generateSortedRuns() {
        TupleWriter tw;
        ArrayList<Tuple> toSort = new ArrayList<>();
        int numRuns = 0;
        Batch batch = base.next();
        int numTuples = 0;

        //read in tuples
        while (batch != null) {
            System.out.println("aaaaa");
            for (int i=0; i<numBuff && batch!=null; i++) {

                //get all tuples in current batch
                for (int j=0;j<batch.size();j++) {
                    toSort.add(batch.get(j));
                    System.out.println("ccccc");
                }
                System.out.println("bbbbb");
                batch = base.next();
            }
            Collections.sort(toSort, new AttrComparator(sortOn));

            //write sorted tuples
            tw = new TupleWriter("SMTEMP-"+numRuns,1000);
            System.out.println("Batchsize:"+batchSize);
            for (Tuple t:toSort) {
                System.out.println("qqqq");
                if (t != null) {
                    tw.next(t);
                }

            }

        }

    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        return true;
    }

    public Object clone() {
        return null;
    }

    class AttrComparator implements Comparator<Tuple> {
        private int attrIndex;

        public AttrComparator(int attrIndex) {
            this.attrIndex = attrIndex;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            return Tuple.compareTuples(t1, t2, attrIndex);
        }
    }



}
