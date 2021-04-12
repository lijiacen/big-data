package big_data.hadoop.mr;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** java中的序列化 */
public class JavaSerialize {

    public static void main(String[] args) {
        StudentJava stu = new StudentJava();
        stu.setName("stu");
        stu.setId(2L);

        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            // 将StudentJava当前对象写入本地文件
            fos = new FileOutputStream("D:\\student.txt");
            oos = new ObjectOutputStream(fos);
            oos.writeObject(stu);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class StudentJava implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long id;
    private String name;

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
