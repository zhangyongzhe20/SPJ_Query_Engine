/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort
    ArrayList<Batch> tempStore;

    public Sort(Operator base, int type) {
        super(type);
        this.base = base;
        tempStore = new ArrayList<>();
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
        base.open();
        Batch b = base.next();
        for(int i = 0; i < 10; i++) { // TODO arbitrary number of tuples
            tempStore.add(b);
            b = base.next();
        }
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

}
