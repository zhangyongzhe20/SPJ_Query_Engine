package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static qp.operators.FileAccess.*;

public class SortMergeJoin extends Join {
    //delete temp files of from external sort
    private static boolean CLEAN_FILES = true;

    // the index of join attribute of left/right tuple
    private int leftAttrIdx;
    private int rightAttrIdx;
    // outBatch size
    private int batchSize;

    // used for file read/write
    private int leftBatchIdx = -1;
    private Batch leftBatch;
    private int rCurBufferIdx = -1;
    private Batch rCurBuffer;
    private int rBufferIdx = 0;
    // size is buffer - 3
    private int rightBufferSize;
    private List<Batch> rightBuffer = new LinkedList<>();
    private List<File> leftFiles;
    private List<File> rightFiles;

    //used in the next() of sort merge join
    private int leftTupleIdx = 0;
    private int rightTupleIdx = 0;
    // marker, used to backtrack when left batch has duplicate join values
    private int markerIndex = -1;
    // end of left stream
    private boolean eosLeft = false;
    // end of right stream
    private boolean eosRight = false;

    // the buffer size of left and right batch
    int rightBatchSize;
    int leftBatchSize;

    public SortMergeJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    @Override
    public boolean open() {
        //calculate the batch size
        batchSize = Batch.getPageSize() / schema.getTupleSize();

        // get the leftIndex and rightIndex;
        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();
        leftAttrIdx = left.getSchema().indexOf(leftAttr);
        rightAttrIdx = right.getSchema().indexOf(rightAttr);

        // joinAttrType = left.getSchema().typeOf(conditionList.get(0).getLhs());
        //left attributes
        ArrayList<Attribute> leftAttrs = new ArrayList<>();
        ArrayList<Attribute> rightAttrs = new ArrayList<>();
        leftAttrs.add(leftAttr);
        rightAttrs.add(rightAttr);
        Sort leftSort = new Sort(left, true, false, leftAttrs, OpType.SORT, 3);
        Sort rightSort = new Sort(right, true, false, rightAttrs, OpType.SORT, 10);
        if (!(leftSort.open() && rightSort.open())) {
            return false;
        }
        try {
            // store sorted intermediate data into disk
            leftFiles = writeOprToFile(leftSort, "L-SMJ");
            rightFiles = writeOprToFile(rightSort, "R-SMJ");

            leftSort.close();
            rightSort.close();
            // init buffer size
            rightBufferSize = getNumBuff() - 3;
            initRBuffer();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        int tupleSize = getRight().getSchema().getTupleSize();
        rightBatchSize = Batch.getPageSize() / tupleSize;
        tupleSize = getLeft().getSchema().getTupleSize();
        leftBatchSize = Batch.getPageSize() / tupleSize;
        return true;
    }
    @Override
    public Batch next() {
        try {
            return nextThrows();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Produces 1 batch of output
     */
    private Batch nextThrows() throws IOException, ClassNotFoundException {
        Batch joinResult = new Batch(batchSize);
        Tuple leftTuple, rightTuple;
        while(!joinResult.isFull() && !eosLeft && !eosRight)
        {
            leftTuple = readNextLeftTuple();
            rightTuple = readNextRightTuple();
            int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIdx, rightAttrIdx);
            if(markerIndex == -1){
                while(comparison < 0) {
                    leftTupleIdx++;
                    leftTuple = readNextLeftTuple();
                    if(leftTuple == null){
                        return joinResult.isEmpty() ? null : joinResult;
                    }
                    comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIdx, rightAttrIdx);
                }
                while(comparison > 0){
                    rightTupleIdx++;
                    rightTuple = readNextRightTuple();
                    if(rightTuple == null){
                        return joinResult.isEmpty() ? null : joinResult;
                    }
                    comparison = Tuple.compareTuples(leftTuple, rightTuple, leftAttrIdx, rightAttrIdx);
                }

                markerIndex = rightTupleIdx;
            }
            if(comparison == 0){
                joinResult.add(leftTuple.joinWith(rightTuple));
                rightTupleIdx++;
                rightTuple = readNextRightTuple();
            }
            else{
                rightTupleIdx = markerIndex;
                leftTupleIdx++;
                leftTuple = readNextLeftTuple();
                markerIndex = -1;
            }
        }
        return joinResult.isEmpty() ? null : joinResult;  // return null to signify end of result
    }

    /**
     * read next left tuple from left batch
     * @return next left tuple
     */
    private Tuple readNextLeftTuple() throws IOException, ClassNotFoundException, IndexOutOfBoundsException {
        int batchIndex = leftTupleIdx / leftBatchSize;
        int tupleIdxInBatch = leftTupleIdx % leftBatchSize;
        // check whether it reaches the end of stream
        Batch curBatch = readLBatch(batchIndex);
        if(curBatch.size() == 0 || curBatch.size() == tupleIdxInBatch){
            eosLeft = true;
            return null;
        }
        return curBatch.get(tupleIdxInBatch);
    }

    /**
     * read next right tuple from right batch
     * @return next right tuple
     */
    private Tuple readNextRightTuple() throws IOException, ClassNotFoundException {
        int batchIndex = rightTupleIdx / rightBatchSize;
        int tupleIdxInBatch = rightTupleIdx % rightBatchSize;
        // check whether it reaches the end of stream
        Batch curBatch = readRBatch(batchIndex);
        if(curBatch.size() == 0 || curBatch.size() == tupleIdxInBatch){
            eosRight = true;
            return null;
        }
        return curBatch.get(tupleIdxInBatch);
    }


    @Override
    public boolean close() {
        if(leftBatch !=null) {
            leftBatch.clear();
        }
        if(rightBuffer!=null) {
            rightBuffer.clear();
        }
        if (CLEAN_FILES) {
            for (File file: leftFiles) {
                file.delete();
            }
            for (File file: rightFiles) {
                file.delete();
            }
        }
        return super.close();
    }

    //  helper functions of file read/write in the below....
    private Batch readLBatch(int idx) throws IOException, ClassNotFoundException, IndexOutOfBoundsException {
        if (idx == leftBatchIdx) {
            return leftBatch;
        }
        File file = leftFiles.get(idx);
        leftBatch = readBatchFromFile(file);
        leftBatchIdx = idx;
        return leftBatch;
    }

    private Batch readRBatch(int idx) throws IOException, ClassNotFoundException {
        if (isInRBuffer(idx)) {
            return readFromBuffer(idx);
        }
        if (idx < rBufferIdx || rightBufferSize == 0) {
            if (rCurBufferIdx == idx) {
                return rCurBuffer;
            }
            rCurBuffer = readBatchFromFile(rightFiles.get(idx));
            rCurBufferIdx = idx;
            return rCurBuffer;
        }
        while (!isInRBuffer(idx)) {
            // move buffer
            int nextRBatchToRead = rBufferIdx + rightBufferSize;
            rightBuffer.remove(0);
            Batch batch = readBatchFromFile(rightFiles.get(nextRBatchToRead));
            rightBuffer.add(batch);
            rBufferIdx++;
        }
        return readFromBuffer(idx);
    }

    private void initRBuffer() throws IOException, ClassNotFoundException {
        rBufferIdx = 0;
        rightBuffer.clear();
        for (int i = 0; i < rightBufferSize; i++) {
            if (i >= rightFiles.size()) {
                break;
            }
            Batch batch = readBatchFromFile(rightFiles.get(i));
            rightBuffer.add(batch);
        }
    }

    private Batch readFromBuffer(int idx) {
        return rightBuffer.get(idx - rBufferIdx);
    }

    private boolean isInRBuffer(int idx) {
        return (rBufferIdx <= idx) && (idx < rBufferIdx + rightBufferSize);
    }
}