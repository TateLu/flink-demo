package demo.flink;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-02-24 18:14
 **/
public class FlinkConstant {

    public static final Map<Class,String> TYPE_MAPPING = new HashMap<>();
    static {
        TYPE_MAPPING.put(Long.class,"long");
        TYPE_MAPPING.put(Integer.class,"long");
        TYPE_MAPPING.put(Double.class,"double");
        TYPE_MAPPING.put(Float.class,"double");
        TYPE_MAPPING.put(LocalDateTime.class,"date");
        TYPE_MAPPING.put(Date.class,"date");
        TYPE_MAPPING.put(String.class,"string");
    }

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";


}
