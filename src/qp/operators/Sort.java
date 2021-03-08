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
    int numRuns = 0;
    TupleReader tr;

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
        if(tr == null) {
            tr = new TupleReader("SMTEMP-0.out", batchSize);
            tr.open();
        }
        Tuple t = tr.next();
        Batch b = new Batch(batchSize);
        while(t != null) {
            b.add(t);
            if(b.isFull()) {
                return b;
            }
            t = tr.next();
        }
        if(b.isEmpty()) {
            return null;
        }
        return b;
    }



    /*Read in numBuff batches and add tuples to arraylist, sort arraylist, write to file*/
    public void generateSortedRuns() {
        TupleWriter tw;
        ArrayList<Tuple> toSort = new ArrayList<>();

        Batch batch = base.next();

        while(batch != null) {
            toSort.clear();
            for(int i = 0; i < 1000; i++) {
                if(batch == null) {
                    break;
                }
                for(int j = 0; j < batch.size(); j++) {
                    toSort.add(batch.get(j));
                }
                batch = base.next();
            }
            if(toSort.isEmpty()) {
                return;
            }
            toSort.sort(new AttrComparator(sortOn));
            tw = new TupleWriter("SMTEMP-0.out", batchSize);
            if (!tw.open()) {
                System.out.println("TupleWriter: Error in opening of TupleWriter");
                System.exit(3);
            }
            for(Tuple t : toSort) {
                tw.next(t);
            }
            tw.close();
            numRuns++;
        }

        // TODO delete sorted runs generated
    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        // TODO
        return true;
    }

    public Object clone() {
        // TODO
        System.out.println("CLONED");
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
