package io.lenses.streamreactor.connect.testcontainers.pojo;

public class Order {
    public Integer id;
    public String product;
    public String created;
    public Double price;
    public Integer qty;

    public Order() { }

    public Order(Integer id, String product, Double price, Integer qty) {
        this.id = id;
        this.product = product;
        this.price = price;
        this.qty = qty;
    }

    public Order(Integer id, String created, String product, Double price, Integer qty) {
        this.id = id;
        this.created = created;
        this.product = product;
        this.price = price;
        this.qty = qty;
    }
}
