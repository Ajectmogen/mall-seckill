package com.macro.mall.portal.domain;

import lombok.Getter;

/**
 * 消息队列枚举类
 * Created by macro on 2018/9/14.
 */
@Getter
public enum QueueEnum {
    /**
     * 消息通知队列
     */
    QUEUE_ORDER_CANCEL("mall1.order.direct", "mall1.order.cancel", "mall1.order.cancel"),
    /**
     * 消息通知ttl队列
     */
    QUEUE_TTL_ORDER_CANCEL("mall1.order.direct.ttl", "mall1.order.cancel.ttl", "mall1.order.cancel.ttl"),
    /**
     * 秒杀订单取消消息队列（秒杀专用，与普通订单队列隔离，避免队头阻塞导致普通订单取消消息被秒杀消息挤住）
     */
    QUEUE_FLASH_ORDER_CANCEL("mall1.flash.direct", "mall1.flash.cancel", "mall1.flash.cancel"),
    /**
     * 秒杀订单取消ttl队列（秒杀专用死信队列）
     */
    QUEUE_TTL_FLASH_ORDER_CANCEL("mall1.flash.direct.ttl", "mall1.flash.cancel.ttl", "mall1.flash.cancel.ttl"),
    /**
     * 秒杀异步建单队列（v2）：消息体为 recordId；消费者 CAS 领取 status 0->10 后建单。
     * 队列耐久，并配置 x-dead-letter-exchange/x-dead-letter-routing-key 指向下面的死信队列。
     */
    QUEUE_FLASH_ORDER_CREATE("mall1.flash.direct", "mall1.flash.create", "mall1.flash.create"),
    /**
     * 秒杀异步建单死信队列（v2）：建单队列的 DLX 目标，用于兜住「消费者明确拒绝」的消息
     */
    QUEUE_FLASH_ORDER_CREATE_DEAD("mall1.flash.direct.ttl", "mall1.flash.dead", "mall1.flash.dead");

    /**
     * 交换名称
     */
    private final String exchange;
    /**
     * 队列名称
     */
    private final String name;
    /**
     * 路由键
     */
    private final String routeKey;

    QueueEnum(String exchange, String name, String routeKey) {
        this.exchange = exchange;
        this.name = name;
        this.routeKey = routeKey;
    }
}