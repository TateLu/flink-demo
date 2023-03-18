package ltz.demo.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @program: flink-demo-1.16
 * @description:
 * @author: TATE.LU
 * @create: 2023-02-08 17:58
 **/
@Data
public class Event implements Serializable {
    private String name;
    private String url;
    private Long size;

    public Event(String name, String url, Long size) {
        this.name = name;
        this.url = url;
        this.size = size;
    }
}
