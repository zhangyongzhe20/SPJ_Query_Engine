/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.*;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort
    ArrayList<Attribute> sortOn;
    int batchSize;
    int numBuff;
    int currFile;
    boolean eof;
    TupleReader tr;
    boolean isDesc;
    Schema schema;
    ArrayList<String> fn;
    String completeFile;

    public Sort(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(type);
        this.base = base;
        this.sortOn = sortOn;
        this.numBuff = numBuff;
        currFile = 0;
        eof = false;
        this.isDesc = isDesc;
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

        this.schema = base.getSchema(); // TODO or should it be this.getSchema()?

        ArrayList<String> filenames = generateSortedRuns();
        System.out.println(filenames);
        fn = filenames;

        while(filenames.size() != 1) {
            ArrayList<ArrayList<String>> runGroups = groupRuns(numBuff - 1, filenames);
            filenames.clear();
            for(ArrayList<String> runGroup : runGroups) {
                System.out.println(runGroup);
                // dont merge last run if it is a single run
                if(runGroup.size() == 1) {
                    continue;
                }
                String output = merge(runGroup);
                filenames.add(output);
            }
        }

        // TODO: set as instance var
        this.completeFile = filenames.get(0);
        System.out.println("Completefile at: " + completeFile);

        System.out.println("Sort.Open() completed successfully");
        return true;
    }

    // runGroup must be at least 2 files in size
    private String merge(ArrayList<String> runGroup) {

        // TODO handle multiple buffers

        // give 1 buffer to each run
        int limit = Math.min(runGroup.size(), numBuff-1); // may waste buffers but check correctness first

        // init output file
        String outname = "next"+runGroup.get(0);
        TupleWriter output = new TupleWriter(outname, batchSize);
        if(!output.open()) {
            System.out.println("Error in TESTFILE.open() in sort.merge()");
            System.exit(3);
        }

        // init tupleReaders
        ArrayList<TupleReader> trs = new ArrayList<>(limit);
        for(int i = 0; i < limit; i++) {
            TupleReader tr = new TupleReader(runGroup.get(i), batchSize);
            if(!tr.open()) {
                System.out.println("Error in treader.open() in sort.merge()");
                System.exit(3);
            }
            trs.add(tr);
        }

        // bring in first batch of tuples
        Tuple[] inMem = new Tuple[trs.size()];
        for(int i = 0; i < trs.size(); i++) {
            inMem[i] = (trs.get(i).next());
        }

        while(true) {
            int i = getIndexOfMinTuple(inMem);

            // clean up resources
            if(i == -1) {
                for(TupleReader tr : trs) {
                    tr.close();
                }
                output.close();
                return outname;
            }

            output.next(inMem[i]);

            inMem[i] = trs.get(i).next();
        }
    }

    // returns -1 to signal no more tuples
    private int getIndexOfMinTuple(Tuple[] inMem) {

        Tuple currSmallest = null;
        for(Tuple t : inMem) {
            if(t != null) {
                currSmallest = t;
                break;
            }
        }
        if(currSmallest == null) {
            return -1;
        }
        // the array has atleast one non-null element

        int indexOfSmallest = -1;
        for(int i = 0; i < inMem.length; i++) {
            if(inMem[i] == null) {
                continue;
            }
            // get index of smaller tuple
            AttrComparator ac = new AttrComparator(sortOn, base.getSchema());
            if(ac.compare(currSmallest, inMem[i]) >= 0) { // TODO someone pls check this
                indexOfSmallest = i;
            }
        }
        return indexOfSmallest;
    }

    // splits filenames into groups of size at most n
    private ArrayList<ArrayList<String>> groupRuns(int n, ArrayList<String> filenames) {
        ArrayList<ArrayList<String>> all = new ArrayList<>();
        ArrayList<String> group = new ArrayList<>();
        for(String file : filenames) {
            group.add(file);
            if(group.size() == n) {
                all.add(group);
                group = new ArrayList<>();
            }
        }
        if(!group.isEmpty()) {
            all.add(group);
        }
        return all;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {

        if(eof) {
            return null;
        }

        if(tr == null) {
            tr = new TupleReader(completeFile, batchSize);
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
                eof = true;
                return b;
            }
            b.add(t);
        }
        return b;
    }



    /*Read in numBuff batches and add tuples to arraylist, sort arraylist, write to file*/
    private ArrayList<String> generateSortedRuns() {
        ArrayList<String> filenames = new ArrayList<>();

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
            toSort.sort(new AttrComparator(sortOn, schema));
            for(Tuple t : toSort) {
                tw.next(t);
                counter++;
            }
            tw.close();
            filenames.add(filename);

            if(flag) {
                System.out.println("GenerateSortedRuns wrote: " + counter + " tuples");
                return filenames;
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
        private ArrayList<Integer> sortOnIndex;

        public AttrComparator(ArrayList<Attribute> sortOn, Schema schema) {
            this.sortOnIndex = new ArrayList<>();
            for(Attribute a : sortOn) {
                int index = schema.indexOf(a);
                sortOnIndex.add(index);
            }
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            if(isDesc) {
                return -Tuple.compareTuples(t1, t2, sortOnIndex, sortOnIndex);
            } else {
                return Tuple.compareTuples(t1, t2, sortOnIndex, sortOnIndex);
            }
        }
    }
}
