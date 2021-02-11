package pers.pao.flink.source.objects;

import lombok.Data;

/**
 * 範例物件
 */
@Data
public class A {
    private final String id;
    private final long num1;
    private final Double num2;

    @Override
    public String toString() {
        return "A{" +
                "id='" + id + '\'' +
                ", num1=" + num1 +
                ", num2=" + num2 +
                '}';
    }
}
