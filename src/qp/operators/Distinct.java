package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Distinct extends Operator {
    Batch incomingBatch;
    Batch outgoingBatch;
    Tuple prev;
    int start;
    ArrayList<Integer> sortOnIndexList;
    Schema schema;
    Operator base;
    ArrayList<Attribute> sortOn;
    boolean isEOS;
    int batchsize;
    boolean isDesc;
    int numBuff;

    public Distinct(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(type);
        this.base = base;
        this.sortOn = sortOn;
        this.schema = base.getSchema();
        this.isDesc = isDesc;
        this.numBuff = numBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < sortOn.size(); ++i)
            newattr.add((Attribute) sortOn.get(i).clone());
        Distinct newproj = new Distinct(newbase, false, isDesc, newattr, optype, numBuff);
        Schema newSchema = newbase.getSchema();
        newproj.setSchema(newSchema);
        return newproj;
    }

    @Override
    public boolean open() {

        isEOS = false;
        start = 0;
        prev = null;

        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        this.sortOnIndexList = new ArrayList<>();
        for(Attribute a : sortOn) {
            int index = schema.indexOf(a);
            sortOnIndexList.add(index);
        }

        if(!base.open()) {
            System.out.println("Cannot open base from distinct");
            System.exit(1);
        }
        return true;
    }
    @Override
    public Batch next() {

        int i;
        if (isEOS) {
            base.close();
            return null;
        }

        outgoingBatch = new Batch(batchsize);

        // Main reading loop. Retrieves batch by batch tuples
        while (!outgoingBatch.isFull()) {
            if (start == 0) {
                incomingBatch = base.next();

                if(incomingBatch != null) {
                    Debug.PPrint(incomingBatch);
                }

                if (incomingBatch == null) {
                    isEOS = true;
                    return outgoingBatch;
                }
            }

            // Keep reading in tuples and then compare to predecessor to check for uniqueness
            for (i = start; i < incomingBatch.size() && (!outgoingBatch.isFull()); i++) {
                Tuple present = incomingBatch.get(i);


                boolean t = prev == null || Tuple.compareTuples(prev, present, sortOnIndexList, sortOnIndexList) != 0;
                // If this is the first tuple read from the sorted batch/different from the last unique tuple present
                if (t) {
                    outgoingBatch.add(present);
                    prev = present;
                }
            }

            if (i == incomingBatch.size())
                start = 0;
            else
                start = i;
        }

        return outgoingBatch;
    }
}
