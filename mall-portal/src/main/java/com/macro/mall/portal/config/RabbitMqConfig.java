package com.macro.mall.portal.config;

import com.macro.mall.portal.domain.QueueEnum;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 消息队列相关配置
 * Created by macro on 2018/9/14.
 */
@Configuration
public class RabbitMqConfig {

    /**
     * 订单消息实际消费队列所绑定的交换机
     */
    @Bean
    DirectExchange orderDirect() {
        return ExchangeBuilder
                .directExchange(QueueEnum.QUEUE_ORDER_CANCEL.getExchange())
                .durable(true)
                .build();
    }

    /**
     * 订单延迟队列队列所绑定的交换机
     */
    @Bean
    DirectExchange orderTtlDirect() {
        return ExchangeBuilder
                .directExchange(QueueEnum.QUEUE_TTL_ORDER_CANCEL.getExchange())
                .durable(true)
                .build();
    }

    /**
     * 订单实际消费队列
     */
    @Bean
    public Queue orderQueue() {
        return new Queue(QueueEnum.QUEUE_ORDER_CANCEL.getName());
    }

    /**
     * 订单延迟队列（死信队列）
     */
    @Bean
    public Queue orderTtlQueue() {
        return QueueBuilder
                .durable(QueueEnum.QUEUE_TTL_ORDER_CANCEL.getName())
                .withArgument("x-dead-letter-exchange", QueueEnum.QUEUE_ORDER_CANCEL.getExchange())//到期后转发的交换机
                .withArgument("x-dead-letter-routing-key", QueueEnum.QUEUE_ORDER_CANCEL.getRouteKey())//到期后转发的路由键
                .build();
    }

    /**
     * 将订单队列绑定到交换机
     */
    @Bean
    Binding orderBinding(DirectExchange orderDirect,Queue orderQueue){
        return BindingBuilder
                .bind(orderQueue)
                .to(orderDirect)
                .with(QueueEnum.QUEUE_ORDER_CANCEL.getRouteKey());
    }

    /**
     * 将订单延迟队列绑定到交换机
     */
    @Bean
    Binding orderTtlBinding(DirectExchange orderTtlDirect,Queue orderTtlQueue){
        return BindingBuilder
                .bind(orderTtlQueue)
                .to(orderTtlDirect)
                .with(QueueEnum.QUEUE_TTL_ORDER_CANCEL.getRouteKey());
    }

    /**
     * 秒杀订单消息实际消费队列所绑定的交换机
     */
    @Bean
    DirectExchange flashOrderDirect() {
        return ExchangeBuilder
                .directExchange(QueueEnum.QUEUE_FLASH_ORDER_CANCEL.getExchange())
                .durable(true)
                .build();
    }

    /**
     * 秒杀订单延迟队列所绑定的交换机
     */
    @Bean
    DirectExchange flashOrderTtlDirect() {
        return ExchangeBuilder
                .directExchange(QueueEnum.QUEUE_TTL_FLASH_ORDER_CANCEL.getExchange())
                .durable(true)
                .build();
    }

    /**
     * 秒杀订单实际消费队列
     */
    @Bean
    public Queue flashOrderQueue() {
        return new Queue(QueueEnum.QUEUE_FLASH_ORDER_CANCEL.getName());
    }

    /**
     * 秒杀订单延迟队列（死信队列）
     */
    @Bean
    public Queue flashOrderTtlQueue() {
        return QueueBuilder
                .durable(QueueEnum.QUEUE_TTL_FLASH_ORDER_CANCEL.getName())
                .withArgument("x-dead-letter-exchange", QueueEnum.QUEUE_FLASH_ORDER_CANCEL.getExchange())//到期后转发的交换机
                .withArgument("x-dead-letter-routing-key", QueueEnum.QUEUE_FLASH_ORDER_CANCEL.getRouteKey())//到期后转发的路由键
                .build();
    }

    /**
     * 将秒杀订单队列绑定到交换机
     */
    @Bean
    Binding flashOrderBinding(DirectExchange flashOrderDirect,Queue flashOrderQueue){
        return BindingBuilder
                .bind(flashOrderQueue)
                .to(flashOrderDirect)
                .with(QueueEnum.QUEUE_FLASH_ORDER_CANCEL.getRouteKey());
    }

    /**
     * 将秒杀订单延迟队列绑定到交换机
     */
    @Bean
    Binding flashOrderTtlBinding(DirectExchange flashOrderTtlDirect,Queue flashOrderTtlQueue){
        return BindingBuilder
                .bind(flashOrderTtlQueue)
                .to(flashOrderTtlDirect)
                .with(QueueEnum.QUEUE_TTL_FLASH_ORDER_CANCEL.getRouteKey());
    }

    /**
     * 秒杀异步建单队列（v2）：durable，消费失败/被拒绝时通过死信交换机转投死信队列
     */
    @Bean
    public Queue flashOrderCreateQueue() {
        return QueueBuilder
                .durable(QueueEnum.QUEUE_FLASH_ORDER_CREATE.getName())
                .withArgument("x-dead-letter-exchange", QueueEnum.QUEUE_FLASH_ORDER_CREATE_DEAD.getExchange())
                .withArgument("x-dead-letter-routing-key", QueueEnum.QUEUE_FLASH_ORDER_CREATE_DEAD.getRouteKey())
                .build();
    }

    /**
     * 秒杀异步建单死信队列（v2）：durable，位于 mall1.flash.direct.ttl 交换机上
     */
    @Bean
    public Queue flashOrderCreateDeadQueue() {
        return QueueBuilder
                .durable(QueueEnum.QUEUE_FLASH_ORDER_CREATE_DEAD.getName())
                .build();
    }

    /**
     * 将秒杀异步建单队列绑定到 mall1.flash.direct 交换机
     */
    @Bean
    Binding flashOrderCreateBinding(DirectExchange flashOrderDirect, Queue flashOrderCreateQueue) {
        return BindingBuilder
                .bind(flashOrderCreateQueue)
                .to(flashOrderDirect)
                .with(QueueEnum.QUEUE_FLASH_ORDER_CREATE.getRouteKey());
    }

    /**
     * 将秒杀异步建单死信队列绑定到 mall1.flash.direct.ttl 交换机
     */
    @Bean
    Binding flashOrderCreateDeadBinding(DirectExchange flashOrderTtlDirect, Queue flashOrderCreateDeadQueue) {
        return BindingBuilder
                .bind(flashOrderCreateDeadQueue)
                .to(flashOrderTtlDirect)
                .with(QueueEnum.QUEUE_FLASH_ORDER_CREATE_DEAD.getRouteKey());
    }

}
