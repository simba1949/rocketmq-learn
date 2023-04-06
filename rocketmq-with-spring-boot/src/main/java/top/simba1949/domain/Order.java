package top.simba1949.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * @author anthony
 * @date 2023/4/6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {
    private static final long serialVersionUID = -5801159102590046241L;

    private String orderCode; // 订单编码
    private List<String> goodList; // 商品列表
}
