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
//                String output = merge(runGroup);
//                filenames.add(output);
            }
        }

        // TODO set sorted filename

        System.out.println("Sort.Open() completed successfully");
        return true;
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
                if(currFile == fn.size() - 1) {
                    eof = true;
                    if(b.isEmpty()) {
                        return null;
                    }
                    return b;
                }
                tr = new TupleReader(fn.get(currFile + 1), batchSize);
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
