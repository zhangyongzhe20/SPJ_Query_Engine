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
    ArrayList<Batch> tempStore;
    int sortOn;
    int batchSize;
    int numBuff;

    public Sort(Operator base, boolean isAsc, boolean isDesc, int sortOn, int type, int numBuff) {
        super(type);
        this.base = base;
        tempStore = new ArrayList<>();
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
        if (!base.open()) {
            return false;
        }

        //Create and prepare tuple reader
        int tupleSize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /*tupleReader = new TupleReader(fileName, batchSize);
        tupleReader.open();*/

        //Get list of attributes(column index) to sort on
        Schema baseSchema = base.getSchema();

        generateSortedRuns();

        /*base.open();
        Batch b = base.next();
        for(int i = 0; i < 10; i++) { // TODO arbitrary number of tuples
            tempStore.add(b);
            b = base.next();
        }*/
        System.out.println("Sort.Open() completed successfully");
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        if(tempStore.isEmpty()) {
            return null;
        }
        return tempStore.remove(0);
    }

    public void generateSortedRuns() {

        int numRuns = 0;
        Batch batch = base.next();
        System.out.println("ssssaaaaas");
        int numTuples = 0;
        while(batch != null) {
            List<Tuple> tuples = batch.getTuples();
            Collections.sort(tuples, new AttrComparator(sortOn));
            System.out.println("Sorted tuples:");



            /*Brick run = new Brick(numBuff, batchSize);
            System.out.println("bbbbb");
            while(!run.isFull() && batch != null) {
                System.out.println("bccccccc");
                run.addBatch(batch);
                batch = base.next();
            }*/

            numRuns++;
            numTuples += tuples.size();


            Brick sortedRun = new Brick(numBuff, batchSize);
            sortedRun.setTuples(tuples);
            File result = writeOutFile(sortedRun, numRuns);
            batch = base.next();
            /*
            sortedFiles.add(result);*/
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

    public File writeOutFile(Brick run, int numRuns) {
        try {
            File temp = new File( "-SMTemp-" + numRuns);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(temp));
            for(Batch batch : run.getBatches()) {
                out.writeObject(batch);
            }
            out.close();
            return temp;
        } catch (IOException io) {
            System.out.println("SortMerge: writing the temporary file error");
        }
        return null;
    }

}
