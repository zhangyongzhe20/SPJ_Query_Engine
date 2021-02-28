package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;

public class SortMerge extends Operator {
    protected Operator base;
    protected int numBuff;
    protected Vector attrSet;
    protected int[] attrIndex;

    protected int batchSize;

    private String fileName;

    protected List<File> sortedFiles;

    protected ObjectInputStream in;

    public SortMerge(Operator base, Vector as, int opType) {
        super(opType);
        this.base = base;
        this.attrSet = as;
//        this.numBuff = numBuff;
//        this.fileName = fileName;
    }

    public SortMerge(Operator base, Vector as, int numBuff, String fileName) {
        super(OpType.SORT);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
        this.fileName = fileName;
    }

    public boolean open() {
//        System.out.println("SortMerge:-----------------in open--------------");
        if(!base.open()) {
            return false;
        } else {
            // Initialization
            int tupleSize = base.getSchema().getTupleSize();
//            System.out.println("tuple size: " + base.getSchema().getAttList());
            batchSize = Batch.getPageSize() / tupleSize;

            /** The followingl loop findouts the index of the columns that
             ** are required from the base operator
             **/
            Schema baseSchema = base.getSchema();
            attrIndex = new int[attrSet.size()];
            for (int i = 0; i < attrSet.size(); i++) {
                Attribute attr = (Attribute) attrSet.elementAt(i);
                int index = baseSchema.indexOf(attr);
                attrIndex[i] = index;
            }

            // Phase 1: Generate sorted runs
            sortedFiles = new ArrayList<>();
//            System.out.println("generate sort runs");
            generateSortedRuns();

            // Phase 2: Merge sorted runs
//            System.out.println("merge sort run: ");
            mergeSortedFiles();

//            testResultFile(sortedFiles.get(0));

            try {
//                File file = new File(fileName);
//                file = sortedFiles
                if(sortedFiles.size() != 1) {
                    return false;
                }
                in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
            } catch (IOException e) {
                System.out.println(" Error reading the file");
                return false;
            }
            return true;
        }
    }

//    public void testResultFile(File file) {
//        try {
//            int times = 0;
//            ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
//            while(in.readObject() != null) {
//                times++;
//                System.out.println(times);
//            }
//            in.close();
//        } catch (IOException|ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }

    public Batch next() {
//        System.out.println("SortMerge:-----------------in next--------------");
        if(sortedFiles.size() != 1) {
            System.out.println("There is something wrong with sort-merge process. ");
        }
        try {
//<<<<<<< HEAD
//            if(in == null)
//                in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
//
//            batch = (Batch) in.readObject();
//        } catch (IOException|ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        if(batch.isEmpty()) {
//=======
            Batch batch = (Batch) in.readObject();
//            System.out.println();
//            System.out.println();
//            Debug.PPrint(batch);
//            System.out.println();
//            System.out.println();
            return batch;
        } catch (IOException e) {
//>>>>>>> 3942488f0d4f70565b1bf87c3a626227a665917b
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("File not found. ");
        }
        return null;
    }

    public boolean close() {
        sortedFiles.get(0).delete();
        try {
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
//        op.close();
        return true;
    }

    public void generateSortedRuns() {
        int numRuns = 0;
        Batch batch = base.next();
        int numTuples = 0;
        while(batch != null) {
            Block run = new Block(numBuff, batchSize);
//            System.out.println(numBuff);
            while(!run.isFull() && batch != null) {
//                System.out.println("batch size: " + batch.size());
                run.addBatch(batch);
                batch = base.next();
            }
//            System.out.println("Whether run is full " + run.isFull());
//            System.out.println("Whether batch is null " + batch == null);
            numRuns++;
            List<Tuple> tuples = run.getTuples();
            numTuples += tuples.size();
            Collections.sort(tuples, new AttrComparator(attrIndex));

            Block sortedRun = new Block(numBuff, batchSize);
            sortedRun.setTuples((Vector) tuples);
//            System.out.println(1);
//            System.out.println("run size: " + tuples.size());
//            if(batch != null) {
//                System.out.println("Batch is not null. " + batch.elementAt(0)._data);
//            }
            File result = writeToFile(sortedRun, numRuns);
            sortedFiles.add(result);
        }
//        System.out.println("numRun: " + numRuns);
//        System.out.println("Size of this run: " + numTuples);
    }

    /**
     * This is the merge part of the whole sort-merge process
     * Recursively merge until there are only one run
     */
    public void mergeSortedFiles() {
        int inputNumBuff = numBuff - 1;
        int mergeTimes = 0;
        List<File> resultSortedFiles;
        while(sortedFiles.size() > 1) {
            resultSortedFiles = new ArrayList<>();
            int mergeNumRuns = 0;
            for(int i = 0; i * inputNumBuff < sortedFiles.size(); i++) {
                // every time sort $(inputNumBuff) files
                int start = i *inputNumBuff;
                int end = (i+1) * inputNumBuff;
                if(end >= sortedFiles.size()) {
                    end = sortedFiles.size();
                }
//                System.out.println("start-end: " + start + " " + end);
                List<File> currentFilesToBeSort = sortedFiles.subList(start, end);
                // merge $(inputNumBuff) runs to a longer run
                File resultFile = mergeSortedRuns(currentFilesToBeSort, mergeTimes, mergeNumRuns);
//                System.out.println("=======================");
//                try {
//                    in = new ObjectInputStream(new FileInputStream(resultFile));
//                    while (true) {
//                        Batch batch = getNextBatch(in);
//                        if (batch == null)
//                            break;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                                    + " " + present.dataAt(2) + " " + present.dataAt(3));
//                        }
//                    }
//                    System.out.println();
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + sortedFiles.get(0));
//                }
//                System.out.println();
                mergeNumRuns++;
                resultSortedFiles.add(resultFile);
            }

            for(File file : sortedFiles) {
                file.delete();
            }
            sortedFiles = resultSortedFiles;

            mergeTimes++;
//            System.out.println("mergeTime: " + mergeTimes);
        }
    }

    public File mergeSortedRuns(List<File> runs, int mergeTimes, int mergeNumRuns) {
        int inputNumBuff = numBuff - 1;
        int numRuns = runs.size();
        int runNum;

        if (inputNumBuff < numRuns) {
            System.out.println("There are too many runs in input buffers. ");
            return null;
        }
        ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        try {
            for (int i = 0; i < numRuns; i++) {
                ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(runs.get(i)));
                inputStreams.add(inputStream);
//                System.out.println("=======================");
//                try {
//                    in = new ObjectInputStream(new FileInputStream(runs.get(i)));
//                    while (true) {
//                        Batch batch = getNextBatch(in);
//                        if (batch == null)
//                            break;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                                    + " " + present.dataAt(2) + " " + present.dataAt(3));
//                        }
//                    }
//                    System.out.println("======end here=========");
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + runs.get(i));
//                }
            }
        } catch (IOException e) {
            System.out.println("Reading the temporary file error");
        }

        // real merging process
        File resultFile = new File(fileName + "-MergedFile-" + mergeTimes + "-" + mergeNumRuns);
        ObjectOutputStream out = initObjectOutputStream(resultFile);

        // we will only use numRun buffer for input even though there could be available buffer
        // because any latter batch can only proceed after all its front batches in the same run
        // has been written to output buffer
        ArrayList<Batch> inputBatches = new ArrayList<>(numRuns);
        for (runNum = 0; runNum < numRuns; runNum++) {
            Batch batch = getNextBatch(inputStreams.get(runNum));
            inputBatches.add(runNum, batch);
            if (batch == null) {
                System.out.println("ERROR: run " + runNum + " is initially empty.");
            }
        }

        Queue<Tuple> inputTuples = new PriorityQueue<>(numRuns, new AttrComparator(attrIndex));
        Map<Tuple, Integer> tupleToRunNumMap = new HashMap<>(numRuns);
        for (runNum = 0; runNum < numRuns; runNum++) {
            Batch batch = inputBatches.get(runNum);
            if(batch != null) {
                Tuple tuple = batch.remove(0);
                inputTuples.add(tuple);
                tupleToRunNumMap.put(tuple, runNum);
                if (batch.isEmpty()) {
                    batch = getNextBatch(inputStreams.get(runNum));
                    inputBatches.set(runNum, batch);
                    //                if (batch == null) {
                    //                    System.out.println("run " + runNum + " has been completely processed");
                    //                }
                }
            }
        }

        Batch outputBuffer = new Batch(batchSize);
//<<<<<<< HEAD
//        int minIndex = 0;
//        // writeObject is the method to write object to output stream
//        int times = 0;
//        while(flag) {
////            if(times >= 20)
////                break;
//            System.out.println(times++);
//            for(int i = 0;i < size;i++) {
//                if(indicator[i] && !inputBatches.get(i).isEmpty()) {
//                    min = inputBatches.get(i).elementAt(0);
//                    minIndex = i;
////                    System.out.println("The min index is " + minIndex);
//                    break;
//                }
//            }
//            for(int i = 0;i < size;i++) {
////                times++;
////                System.out.println(times);
////                System.out.println(i);
//                if(indicator[i]) {
//                    Batch currBatch = inputBatches.get(i);
//                    if (currBatch.isEmpty()) {
//                        System.out.println(i);
//                        try {
//                            currBatch = (Batch) inputStreams.get(i).readObject();
////                            System.out.println(inputBatches.size());
//                            inputBatches.remove(i);
////                            System.out.println(inputBatches.size());
//                            inputBatches.add(i, currBatch);
//
//                        } catch (EOFException e) {
//                            System.out.println("Reading input streams error " + i);
//                            try {
//                                inputStreams.get(i).close();
//                            } catch (IOException e1) {
//                                e1.printStackTrace();
//                            }
//                            indicator[i] = false;
////                            break;
//                        } catch (IOException|ClassNotFoundException e) {
//                            System.out.println("Class not found. ");
//                        }
//                    }
//                    if(indicator[i]) {
//                        Tuple curr = currBatch.elementAt(0);
//                        if (Tuple.compareTuples(min, curr, attrIndex) == 1) {
//                            min = curr;
//                            minIndex = i;
//                        }
//                    }
//                }
//            }
//            outputBuffer.add(min);
////            System.out.println(minIndex);
//            if(outputBuffer.isFull()) {
//                try {
//                    ObjectOutputStream out = new AppendableObjectOutputStream(new FileOutputStream(resultFile));
//                    out.writeObject(outputBuffer);
//                    out.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                outputBuffer = new Batch(batchSize);
//            }
//            if(!inputBatches.get(minIndex).isEmpty()) {
//                inputBatches.get(minIndex).remove(0);
//                System.out.println("Size is " + inputBatches.get(minIndex).size() + "-" + minIndex);
//=======
        Batch batch;

        while (!inputTuples.isEmpty()) {
            // output minTuple to output buffer and write out result if outputBuffer is full
            Tuple minTuple = inputTuples.remove();
//            System.out.println(minTuple._data);
            outputBuffer.add(minTuple);
            if (outputBuffer.isFull()) {
                appendToObjectOutputStream(out, outputBuffer);
                outputBuffer.clear();
//                System.out.println("==========final result=============");
//                try {
//                    ObjectInputStream a = new ObjectInputStream(new FileInputStream(resultFile));
//                    while (true) {
//                        batch = getNextBatch(a);
//                        if (batch == null)
//                            break;
//                        for (int j = 0; j < batch.size(); j++) {
//                            Tuple present = batch.elementAt(j);
//                            System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                                    + " " + present.dataAt(2) + " " + present.dataAt(3));
//                        }
//                    }
//                    System.out.println("=========end=============");
//                } catch (Exception e) {
//                    System.err.println(" Error reading " + resultFile);
//                }
//                System.out.println();
            }

            // add a new tuple from the same run into priority queue if exists
            runNum = tupleToRunNumMap.get(minTuple);
            batch = inputBatches.get(runNum);

            if (batch != null) {
//                System.out.println("batch " + runNum + " size " + batch.size());
                // notEmpty of batch is ensured by the algorithm
                Tuple tuple = batch.remove(0);
                inputTuples.add(tuple);
                tupleToRunNumMap.put(tuple, runNum);
                if (batch.isEmpty()) {
                    batch = getNextBatch(inputStreams.get(runNum));
                    inputBatches.set(runNum, batch);
//                    if (batch == null) {
//                        System.out.println("run " + runNum + " has been completely processed");
//                    }
                }
            }
        }

        //add the leftover in output buffer
        if(!outputBuffer.isEmpty()) {
//            System.out.println("add leftover");
            appendToObjectOutputStream(out, outputBuffer);
            outputBuffer.clear();
        }
        tupleToRunNumMap.clear();
        closeObjectOutputStream(out);
//        System.out.println("==========final result=============");
//        try {
//            ObjectInputStream a = new ObjectInputStream(new FileInputStream(resultFile));
//            while (true) {
//                batch = getNextBatch(a);
//                if (batch == null)
//                    break;
//                for (int j = 0; j < batch.size(); j++) {
//                    Tuple present = batch.elementAt(j);
//                    System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                            + " " + present.dataAt(2) + " " + present.dataAt(3));
//                }
//            }
//            System.out.println();
//        } catch (Exception e) {
//            System.err.println(" Error reading " + resultFile);
//        }
//        System.out.println();
        return resultFile;
    }


    protected Batch getNextBatch(ObjectInputStream inputStream) {
        try {
            Batch batch = (Batch) inputStream.readObject();
            if(batch.isEmpty()) {
                System.out.println("batch is empty");
            }
            return batch;
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found. ");
        }
        return null;
    }

    public File writeToFile(Block run, int numRuns) {
        try {
            File temp = new File(fileName + "-SMTemp-" + numRuns);
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

    public ObjectOutputStream initObjectOutputStream(File file) {
        try {
            return new ObjectOutputStream(new FileOutputStream(file, true));
        } catch (IOException io) {
            System.out.println("SortMerge: cannot initialize object output stream");
        }
        return null;
    }

    public void appendToObjectOutputStream(ObjectOutputStream out, Batch batch) {
        try {
            out.writeObject(batch);
            out.reset();          //reset the ObjectOutputStream to enable appending result
//            System.out.println("=========append result==============");
//            for (int j = 0; j < batch.size(); j++) {
//                Tuple present = batch.elementAt(j);
//                System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
//                        + " " + present.dataAt(2) + " " + present.dataAt(3));
//            }
//            System.out.println("===========end============");
        } catch (IOException io) {
            System.out.println("SortMerge: encounter error when append to object output stream");
        }
    }

    public void closeObjectOutputStream(ObjectOutputStream out) {
        try {
            out.close();
        } catch (IOException io) {
            System.out.println("SortMerge: cannot close object output stream");
        }
    }

    class AttrComparator implements Comparator<Tuple> {
        private int[] attrIndex;

        public AttrComparator(int[] attrIndex) {
            this.attrIndex = attrIndex;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            return Tuple.compareTuples(t1, t2, attrIndex);
        }
    }

    /** number of buffers available to this join operator **/

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }
}