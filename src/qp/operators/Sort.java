/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort

    public Sort(Operator base, int type) {
        super(type);
        this.base = base;
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
        Debug.PPrint(b); // should be non-null
        for(int i = 0; i < b.size(); i++) {
            Debug.PPrint(b.get(i)); // should be non-null
        }
        return true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        System.out.println("Sort.next() called");
        return null;
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
