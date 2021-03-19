/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    public static int numBuffer;
    public static int numJoin;

    public static int getBuffersPerJoin() {
        if(numJoin == 0) {
            return numBuffer;
        }
        return numBuffer / numJoin;
    }

    public static int getBuffers() {
        return numBuffer;
    }
}
