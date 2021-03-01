/**
 * Block represents some sequential Batches
 **/

package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Block implements Serializable {

    int MAX_SIZE;             // Number of batches per block
    static int BlockSize;      // Number of bytes per block
    ArrayList<Batch> batches;  // The batches in the block

    /** Set number of bytes per block **/
    public static void setBlockSize(int size) {
        BlockSize = size;
    }

    /** Get number of bytes per block **/
    public static int getBlockSize() {
        return BlockSize;
    }

    /** Number of batches per block **/
    public Block(int numbatch) {
        MAX_SIZE = numbatch;
        batches = new ArrayList<>(MAX_SIZE);
    }

    /** Insert the batch in block at next free location **/
    public void add(Batch b) {
        batches.add(b);
    }

    public int capacity() {
        return MAX_SIZE;
    }

    public void clear() {
        batches.clear();
    }

    public boolean contains(Batch b) {
        return batches.contains(b);
    }

    public Batch get(int i) {
        return batches.get(i);
    }

    public int indexOf(Batch b) {
        return batches.indexOf(b);
    }

    public void add(Batch b, int i) {
        batches.add(i, b);
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }

    public void remove(int i) {
        batches.remove(i);
    }

    public void set(Batch b, int i) {
        batches.set(i, b);
    }

    public int size() {
        return batches.size();
    }

    public boolean isFull() {
        if (size() == capacity())
            return true;
        else
            return false;
    }
}