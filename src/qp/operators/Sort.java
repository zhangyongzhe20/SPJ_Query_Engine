/**
 * Sorts the base relational table
 **/

package qp.operators;

import qp.utils.Batch;

/**
 * Sort operator - sorts data from a file
 */
public class Sort extends Operator {

    Operator base;                 // Base table to sort

    public Sort(int type) {
        super(type);
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
        return false;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        return null;
    }

    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        return false;
    }

    public Object clone() {
        return null;
    }

}
