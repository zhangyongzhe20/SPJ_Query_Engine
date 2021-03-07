/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
    protected TupleWriter tupleWriter;
    protected int[] attrIndex;
    protected List<File> sortedFiles;


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
//        return true;
        if (!base.open()) {
            return false;
        }

        //Create and prepare tuple reader
        int tupleSize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        tupleReader = new TupleReader(fileName, batchSize);
        tupleReader.open();

        //Get list of attributes(column index) to sort on
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attributeArrayList.size()];
        for (int i = 0; i < attributeArrayList.size(); i++) {
            Attribute attr = attributeArrayList.get(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;
        }

        generateSortedRuns();

        return true;

    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        return null;
    }

    public void generateSortedRuns() {
        int numRuns = 0;
        Block run = new Block(numBuff, batchSize);
        tupleReader.open();

        //read tuples into a batch
        while (!tupleReader.isEOF()) {
            Batch batch = new Batch(batchSize);
            while (!batch.isFull()) {
                batch.add(tupleReader.next());
            }

            //Add the newly filled batch to the block
            run.add(batch);

            }
        //Sort the block
        numRuns++;
        System.out.println("Starting block sort");
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (int i=0; i<run.size(); i++) {
            tuples.add(run.get(i));
        }

        System.out.println("Tuples read. Ready to sort");
        tuples.sort((o1, o2) -> Tuple.compareTuples(o1,o2,attrIndex));

       /* //Create block to hold sorted tuples and write out to file
        Block sortedRun = new Block(numBuff, batchSize);
        sortedRun.setTuples(tuples);*/

        String writeFile = fileName + "-SMTemp-" + numRuns;
        tupleWriter = new TupleWriter(writeFile,batchSize);
        tupleWriter.open();
        for (Tuple t:tuples) {
            tupleWriter.next(t);
        }
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
