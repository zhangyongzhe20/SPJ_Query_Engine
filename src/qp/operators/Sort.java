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
    ArrayList<String> filenames;
    int currFile;
    boolean eof;
    TupleReader tr;

    public Sort(Operator base, boolean isAsc, boolean isDesc, int sortOn, int type, int numBuff) {
        super(type);
        this.base = base;
        this.sortOn = sortOn;
        this.numBuff = numBuff;
        filenames = new ArrayList<>();
        currFile = 0;
        eof = false;
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
        if(!base.open()) {
            System.out.println("Error in base.open() in sort");
            System.exit(3);
        }

        //Create and prepare tuple reader
        int tupleSize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        generateSortedRuns();


        System.out.println("Sort.Open() completed successfully");
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {

        if(eof) {
            return null;
        }

        if(tr == null) {
            tr = new TupleReader("SMTEMP-0.out", batchSize);
            if(!tr.open()) {
                System.out.println("Error opening tr in Sort.next()");
                System.exit(3);
            }
        }

        Batch b = new Batch(batchSize);
        while(!b.isFull()) {
            Tuple t = tr.next();
            if(t == null) {
                tr.close();
                if(currFile == filenames.size() - 1) {
                    eof = true;
                    if(b.isEmpty()) {
                        return null;
                    }
                    return b;
                }
                tr = new TupleReader(filenames.get(currFile + 1), batchSize);
                if(!tr.open()) {
                    System.out.println("Error opening tr in Sort.next()");
                    System.exit(3);
                }
                currFile++;
                break;
            }
            b.add(t);
        }
        return b;
    }



    /*Read in numBuff batches and add tuples to arraylist, sort arraylist, write to file*/
    public void generateSortedRuns() {
        TupleWriter tw;
        ArrayList<Tuple> toSort = new ArrayList<>();

        System.out.println("numbuff = " + numBuff);
        int counter = 0;
        boolean flag = false;

        while(true) {

            String filename = "SMTEMP-"+filenames.size()+".out";
            tw = new TupleWriter(filename, batchSize);
            if (!tw.open()) {
                System.out.println("TupleWriter: Error in opening of TupleWriter");
                System.exit(3);
            }

            toSort.clear();

            for(int i = 0; i < numBuff; i++) {
                Batch batch = base.next();
                if(batch == null) {
                    flag = true;
                    break;
                }
                for(int j = 0; j < batch.size(); j++) {
                    toSort.add(batch.get(j));
                }
            }
            toSort.sort(new AttrComparator(sortOn));
            for(Tuple t : toSort) {
                tw.next(t);
                counter++;
            }
            tw.close();
            filenames.add(filename);

            if(flag) {
                System.out.println("GenerateSortedRuns wrote: " + counter + " tuples");
                return;
            }
        }
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
