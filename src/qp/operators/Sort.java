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
        Batch b = base.next();
        while(b != null) {
            tempStore.add(b);
            b = base.next();
        }
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        if(tempStore.isEmpty()) {
            return null;
        }
        Batch b = tempStore.get(0);
        tempStore.remove(0);
        return b;
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
