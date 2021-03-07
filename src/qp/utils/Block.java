/**
 * Block represents some sequential Batches
 **/

package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Block implements Serializable {

    int NUM_BATCH;
    int NUM_TUPLES = -1;
    int pageSize = -1;
    ArrayList<Batch> batches;  // The batches in the block
    ArrayList<Tuple> tuples;

    /** Number of batches per block, Batch must not be null **/
    public Block(int numbatch, int pageSize) {
        batches = new ArrayList<>(numbatch);
        NUM_BATCH = numbatch;
        this.pageSize = pageSize;
        tuples = new ArrayList<>(numbatch * pageSize);
    }

    /** Insert the batch in block at next free location **/
    public void add(Batch b) {
        batches.add(b);
    }

    // assumes that all batches except last are Batch.MAX_SIZE
    public Tuple get(int i) {
        int c = batches.get(0).capacity();
        int bindex = i / c;
        int index = i % c;
        return batches.get(bindex).get(index);
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }

    // refers to num TUPLES within all batches NOT num pages
    public int size() {
        if (NUM_TUPLES != -1) {
            return NUM_TUPLES;
        }

        NUM_TUPLES = 0;
        for(Batch b : batches) {
            NUM_TUPLES += b.size();
        }
        return NUM_TUPLES;
    }

    // checks if block is filled with maximum pages
    public boolean isBatchesFull() {
        if (batches.size() == NUM_BATCH)
            return true;
        else
            return false;
    }

    public void setTuples(ArrayList<Tuple> tupleList) {
        Batch batch = new Batch(pageSize);
        for(int i = 0;i < tupleList.size();i++) {
            if(batch.isFull()) {
                batches.add(batch);
                batch = new Batch(pageSize);
            }
            batch.add((Tuple) tupleList.get(i));
            tuples.add((Tuple) tupleList.get(i));
        }
        if(!batch.isEmpty()) {
            batches.add(batch);
        }
    }
}