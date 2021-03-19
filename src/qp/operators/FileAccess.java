package qp.operators;

import qp.utils.Batch;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileAccess {
    public static List<File> writeOprToFile(Operator operator, String prefix) throws IOException {
        Batch batch;
        int count = 0;
        List<File> files = new ArrayList<>();
        while ((batch = operator.next()) != null) {
            File file = new File(prefix + "_" + count);
            count += 1;
            writeBatchToFile(batch, file);
            files.add(file);
        }
        return files;
    }

    public static void writeBatchToFile(Batch batch, File file) throws IOException {
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
        objectOutputStream.writeObject(batch);

    }
    public static Batch readBatchFromFile(File file) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
        return (Batch) objectInputStream.readObject();
    }
}
