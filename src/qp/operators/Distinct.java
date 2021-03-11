package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Distinct extends Sort{
    Batch incomingBatch;
    Batch outgoingBatch;
    Tuple prev;
    int start;
    ArrayList<Integer> sortOnIndexList;
    Schema schema;
    Operator base;
    ArrayList<Attribute> sortOn;
    boolean isEOS;

    public Distinct(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(base, isAsc, isDesc, sortOn, type, numBuff);
        this.base = base;
        this.sortOn = sortOn;
    }

    @Override
    public boolean open() {
        System.out.println("DISTINCT CALLED OPEN");
        isEOS = false;
        start = 0;
        prev = null;
        this.schema = base.getSchema();
        this.sortOnIndexList = new ArrayList<>();
        for(Attribute a : sortOn) {
            if (schema == null) System.out.println("SCHEMA NULL");
            int index = schema.indexOf(a);
            sortOnIndexList.add(index);
        }
        return super.open();
    }
    @Override
    public Batch next() {
        System.out.println("DISTINCT CALLED NEXT");
        int i;
        if (isEOS) {
            super.close();
            return null;
        }

        outgoingBatch = new Batch(batchSize);

        // Main reading loop. Retrieves batch by batch tuples
        while (!outgoingBatch.isFull()) {
            if (start == 0) {
                incomingBatch = super.next();

                if (incomingBatch == null) {
                    isEOS = true;
                    return outgoingBatch;
                }
            }

            // Keep reading in tuples and then compare to predecessor to check for uniqueness
            for (i = start; i < incomingBatch.size() && (!outgoingBatch.isFull()); i++) {
                Tuple present = incomingBatch.get(i);


                // If this is the first tuple read from the sorted batch/different from the last unique tuple present
                if (prev == null || Tuple.compareTuples(prev, present, sortOnIndexList, sortOnIndexList) != 0) {
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
