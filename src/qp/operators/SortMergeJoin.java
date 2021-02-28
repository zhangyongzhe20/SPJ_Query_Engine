package qp.operators;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class SortMergeJoin extends Join{
    private Batch leftBatch;
    private Batch rightBatch;
    // index of left buffer
    private int leftBatchIndex = 0;
    // index of right buffer
    private int rightBatchIndex = 0;
    private Tuple leftTuple = null;
    private Tuple rightTuple = null;
    // the index of join attribute of left tuple
    private int leftTupleIndex = 0;
    // the index of join attribute of right tuple
    private int rightTupleIndex = 0;
    // the data type of the join attribute
    private int joinAttrType;
    // marker, used to backtrack when left batch has duplicate join values
    private int markerIndex = -1;
    // end of left stream
    private boolean eosLeft = false;
    // end of right stream
    private boolean eosRight = false;
    // others
    private int batchSize;


    /**
     * Constructor of sort-merge join based on block nest loop join
     * @param basejn
     */
    public SortMergeJoin(Join basejn) {
        super(basejn.getLeft(), basejn.getRight(), basejn.getCondition(), basejn.getOpType());
        //TODO others
        schema = basejn.getSchema();
    }

    /**
     *
     * @return true if the operator open successfully
     */
    @Override
    public boolean open(){
        //TODO: External sort here
        left.open();
        right.open();
        //calculate the batch size
        batchSize = Batch.getPageSize() / schema.getTupleSize();
        // get the leftIndex and rightIndex; TODO: only implemented one condition
        leftTupleIndex = left.getSchema().indexOf(getCondition().getLhs());
        rightTupleIndex = right.getSchema().indexOf((Attribute) getCondition().getRhs());
        // get the join attribute; TODO: only implemented one condition
        joinAttrType = left.getSchema().typeOf(conditionList.get(0).getLhs());
        return super.open();
    }

    @Override
    public Batch next(){
        Batch output = new Batch(batchSize);
        //initial left and right tuple with index = 0
        leftTuple = readNextLeftTuple();
        rightTuple = readNextRightTuple();
        while(!output.isFull() && !eosLeft && !eosRight){
            int comparsion = compareLeftRightTuple(leftTuple, rightTuple, leftTupleIndex, rightTupleIndex);
            // no marker at the beginning
            if(markerIndex == -1) {
                while (comparsion < 0) {
                    leftTuple = readNextLeftTuple();
                    comparsion = compareLeftRightTuple(leftTuple, rightTuple, leftTupleIndex, rightTupleIndex);
                }
                while (comparsion > 0) {
                    rightTuple = readNextRightTuple();
                    comparsion = compareLeftRightTuple(leftTuple, rightTuple, leftTupleIndex, rightTupleIndex);
                }
                // need to reverse one
                markerIndex = rightBatchIndex - 1;
            }
            if(comparsion == 0){
             //join left and right, then insert to output
             output.add(leftTuple.joinWith(rightTuple));
             rightTuple = readNextRightTuple();
            }else{
                rightBatchIndex = markerIndex;
                leftTuple = readNextLeftTuple();
                markerIndex = -1;
            }
        }
        return output;
    }

    /**
     *
     * @return true if operator closed sucessfully
     */
    @Override
    public boolean close(){
        left.close();
        right.close();
        return super.close();
    }



    /**
     * read next left tuple from left batch
     * @return next left tuple
     */
    private Tuple readNextLeftTuple() {
        if(leftBatch == null || left.next() == null){
            eosLeft = true;
            return null;
        }
        // if current left batch index reach the end, fetch new batch
        if(leftBatchIndex == leftBatch.size()){
            leftBatch = left.next();
            leftBatchIndex = 0;
        }

        return leftBatch.get(leftBatchIndex++);
    }

    /**
     * read next right tuple from right batch
     * @return next right tuple
     */
    private Tuple readNextRightTuple() {
        if(rightBatch == null || right.next() == null){
            eosRight = true;
            return null;
        }
        // if current right batch index reach the end, fetch new batch
        if(rightBatchIndex == rightBatch.size()){
            rightBatch = right.next();
            rightBatchIndex = 0;
        }

        return rightBatch.get(rightBatchIndex++);
    }

    /**
     * compare the join attribute of left and right tuple
     * @param left
     * @param right
     * @param leftIndex
     * @param rightIndex
     * @return
     */
    private int compareLeftRightTuple(Tuple left, Tuple right, int leftIndex, int rightIndex){
        Object leftValue = left.dataAt(leftIndex);
        Object rightValue = right.dataAt(rightIndex);

        switch (joinAttrType) {
            case Attribute.INT:
                return Integer.compare((int) leftValue, (int) rightValue);
            case Attribute.STRING:
                return ((String) leftValue).compareTo((String) rightValue);
            case Attribute.REAL:
                return Float.compare((float) leftValue, (float) rightValue);
            //TODO: if needed, can add more attributes here such as TIME
            default:
                return 0;
        }
    }


    public static void main(String[] args) {
        //node is the query plan
        Operator node = null;
        int numOfBuff = 3;
        SortMergeJoin smj = new SortMergeJoin((Join) node);

        //TODO: external merge sort for left batch
        //List<Attribute> leftAttrs = new ArrayList<>();
        //leftAttrs.add(smj.getCondition().getLhs());
        //smj.setLeft(new externalMergeSort(left, leftAttrs, numOfBuff));
        //TODO: external merge sort for right batch
        //List<Attribute> rightAttrs = new ArrayList<>();
        //rightAttrs.add((Attribute) smj.getCondition().getRhs());
        //smj.setRight(new externalMergeSort(right, rightAttrs, numOfBuff));

        smj.setNumBuff(numOfBuff);
    }
}
