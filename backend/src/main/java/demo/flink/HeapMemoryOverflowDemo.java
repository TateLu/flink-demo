package demo.flink;

/**
 * @program: flink-demo
 * @description:
 * @author: TATE.LU
 * @create: 2024-03-14 13:03
 **/
import java.util.ArrayList;
import java.util.List;

public class HeapMemoryOverflowDemo {

    //增加 jvm参数 -Xmx100m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=C:\Users\ltz2y\Downloads

    static class LargeObject {
        private double[] data = new double[100000]; // 大对象，占用较多内存
    }

    public static void main(String[] args) {
        List<LargeObject> list = new ArrayList<>();

        try {
            while (true) {
                // 不断添加大对象到列表中
                list.add(new LargeObject());
            }
        } catch (OutOfMemoryError e) {
            System.out.println("发生堆内存溢出异常: " + e.getMessage());
        }
    }
}
