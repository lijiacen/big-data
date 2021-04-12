package big_data.hadoop.mr;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class HadoopSerialize {
    public static void main(String[] args) {
        //
        StudentWritable stu = new StudentWritable();
        stu.setId(3L);
        stu.setName("hbase");

        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            // 将StudentJava当前对象写入本地文件
            fos = new FileOutputStream("D:\\student_hadoop.txt");
            oos = new ObjectOutputStream(fos);
            stu.write(oos);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class StudentWritable implements Writable {
    private Long id;
    private String name;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
