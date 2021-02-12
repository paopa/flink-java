package pers.pao.flink.transform.objects;

import lombok.*;

/**
 * 範例物件
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class A {

    private String id;
    private long num1;
    private Double num2;

    @Override
    public String toString() {
        return "A{" +
                "id='" + id + '\'' +
                ", num1=" + num1 +
                ", num2=" + num2 +
                '}';
    }
}
