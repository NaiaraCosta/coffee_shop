package br.com.coffee.shop.util;

import br.com.coffee.shop.generated.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class CoffeeOrderUtil {

    public static CoffeeOrder buildNewCoffeeOrder() {

        // var orderId = OrderId.newBuilder().setId(randomId()).build();

        var orderId=   OrderId.newBuilder().setId(randomId()).build();

        return CoffeeOrder.newBuilder()
                // .setId(randomId())
                // .setId(orderId)
                .setId(UUID.randomUUID())
                .setName("Naiara Costa")
                .setStore(generateStore())
                .setOrderLineItems(generateOrderLineItems())
                .setOrderTime(Instant.now())
                .setOrderDate(LocalDate.now())
                .setStatus("NEW")
                .setPickUp(PickUp.IN_STORE)
                .build();
    }

    private static List<OrderLineItem> generateOrderLineItems() {
        var orderLineItem = OrderLineItem.newBuilder()
                .setName("Caffe Latte")
                .setQuantity(1)
                .setSize(Size.MEDIUM)
                .setCost(BigDecimal.valueOf(3.99))
                .build();
        return List.of(orderLineItem);
    }

    private static Store generateStore() {
        return Store.newBuilder()
                .setId(randomId())
                .setAddress(buildAddress())
                .build();
    }

    private static Address buildAddress() {
        return Address.newBuilder()
                .setAddress("193 Guaira Street")
                .setCity("São José dos Pinhais")
                .setStateProvince("PR")
                .setZip("83030-591")
                .build();
    }

    public static int randomId() {
        Random random = new Random();
        return random.nextInt(1000);
    }

}