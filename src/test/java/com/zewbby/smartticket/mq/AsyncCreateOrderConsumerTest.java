package com.zewbby.smartticket.mq;

import com.zewbby.smartticket.constant.ErrorMessageConstant;
import com.zewbby.smartticket.constant.OrderConstant;
import com.zewbby.smartticket.config.AsyncOrderSubmitProperties;
import com.zewbby.smartticket.config.MqConsumerProperties;
import com.zewbby.smartticket.config.StockBucketProperties;
import com.zewbby.smartticket.domain.dto.OrderRequestSuccessBind;
import com.zewbby.smartticket.domain.dto.OrderSnapshot;
import com.zewbby.smartticket.domain.dto.StockDecreaseCommand;
import com.zewbby.smartticket.domain.entity.TicketOrder;
import com.zewbby.smartticket.domain.entity.TicketOrderAudience;
import com.zewbby.smartticket.domain.entity.TicketOrderRequest;
import com.zewbby.smartticket.domain.entity.TicketPurchasePlanAudience;
import com.zewbby.smartticket.domain.entity.UserAccount;
import com.zewbby.smartticket.enums.CompensationStatusEnum;
import com.zewbby.smartticket.enums.ConsumerExceptionTypeEnum;
import com.zewbby.smartticket.enums.OrderRequestStatusEnum;
import com.zewbby.smartticket.enums.OrderStatusEnum;
import com.zewbby.smartticket.enums.RedisStockReleaseResult;
import com.zewbby.smartticket.mapper.OrderMapper;
import com.zewbby.smartticket.mapper.OrderRequestMapper;
import com.zewbby.smartticket.mapper.TicketCategoryMapper;
import com.zewbby.smartticket.mapper.TicketStockBucketMapper;
import com.zewbby.smartticket.mapper.TicketStockMapper;
import com.zewbby.smartticket.mapper.TicketOrderAudienceMapper;
import com.zewbby.smartticket.mapper.TicketPurchasePlanAudienceMapper;
import com.zewbby.smartticket.mapper.TicketPurchasePlanMapper;
import com.zewbby.smartticket.mapper.UserMapper;
import com.zewbby.smartticket.service.AsyncOrderInFlightService;
import com.zewbby.smartticket.service.DeadLetterMessageService;
import com.zewbby.smartticket.service.ObservabilityMetricsService;
import com.zewbby.smartticket.service.OrderSnapshotCacheService;
import com.zewbby.smartticket.service.ShowRelationCacheService;
import com.zewbby.smartticket.service.StockLuaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AsyncCreateOrderConsumerTest {

    @Mock
    private OrderRequestMapper orderRequestMapper;

    @Mock
    private OrderMapper orderMapper;

    @Mock
    private UserMapper userMapper;

    @Mock
    private TicketCategoryMapper ticketCategoryMapper;

    @Mock
    private TicketStockMapper ticketStockMapper;

    @Mock
    private TicketStockBucketMapper ticketStockBucketMapper;

    @Mock
    private OrderTimeoutProducer orderTimeoutProducer;

    @Mock
    private StockLuaService stockLuaService;

    @Mock
    private DeadLetterMessageService deadLetterMessageService;

    @Mock
    private ObservabilityMetricsService observabilityMetricsService;

    @Mock
    private ShowRelationCacheService showRelationCacheService;

    @Mock
    private AsyncOrderInFlightService asyncOrderInFlightService;

    @Mock
    private OrderSnapshotCacheService orderSnapshotCacheService;

    @Mock
    private TicketPurchasePlanMapper ticketPurchasePlanMapper;

    @Mock
    private TicketPurchasePlanAudienceMapper ticketPurchasePlanAudienceMapper;

    @Mock
    private TicketOrderAudienceMapper ticketOrderAudienceMapper;

    private AsyncCreateOrderConsumer consumer;

    @BeforeEach
    void setUp() {
        MqConsumerProperties mqConsumerProperties = new MqConsumerProperties();
        mqConsumerProperties.setProcessingTimeoutSeconds(120);
        StockBucketProperties stockBucketProperties = new StockBucketProperties();
        stockBucketProperties.setEnabled(false);
        consumer = new AsyncCreateOrderConsumer(
                orderRequestMapper,
                orderMapper,
                userMapper,
                ticketCategoryMapper,
                ticketStockMapper,
                ticketStockBucketMapper,
                orderTimeoutProducer,
                stockLuaService,
                deadLetterMessageService,
                mqConsumerProperties,
                observabilityMetricsService,
                stockBucketProperties,
                null,
                null,
                null,
                asyncOrderInFlightService
        );
        ReflectionTestUtils.setField(consumer, "ticketPurchasePlanMapper", ticketPurchasePlanMapper);
        ReflectionTestUtils.setField(consumer, "ticketPurchasePlanAudienceMapper", ticketPurchasePlanAudienceMapper);
        ReflectionTestUtils.setField(consumer, "ticketOrderAudienceMapper", ticketOrderAudienceMapper);
    }

    @Test
    void planOrderCopiesExactAudienceSnapshotsBeforeRequestSuccess() {
        TicketOrderRequest queued = queuedRequest();
        queued.setPurchasePlanId(90L);
        queued.setQuantity(2);
        TicketOrderRequest processing = processingRequest();
        processing.setPurchasePlanId(90L);
        processing.setQuantity(2);
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 2)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(ticketPurchasePlanAudienceMapper.selectByPlanIdAndSelectionType(90L, "SELECTED"))
                .thenReturn(List.of(
                        new TicketPurchasePlanAudience(1L, 90L, 301L, "SELECTED", 1, "甲", "hash-a", LocalDateTime.now()),
                        new TicketPurchasePlanAudience(2L, 90L, 302L, "SELECTED", 2, "乙", "hash-b", LocalDateTime.now())
                ));
        when(ticketOrderAudienceMapper.insertBatch(anyList())).thenReturn(2);
        when(ticketPurchasePlanMapper.markOrderCreated(anyLong(), anyString(), anyLong(), anyString(), any()))
                .thenReturn(1);
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        ArgumentCaptor<List<TicketOrderAudience>> audienceCaptor = ArgumentCaptor.forClass(List.class);
        org.mockito.InOrder order = inOrder(ticketOrderAudienceMapper, ticketPurchasePlanMapper, orderRequestMapper);
        order.verify(ticketOrderAudienceMapper).insertBatch(audienceCaptor.capture());
        order.verify(ticketPurchasePlanMapper).markOrderCreated(eq(90L), eq("REQ1"), eq(200L), eq("ORDER_CREATED"), any());
        order.verify(orderRequestMapper).markSuccess(10L, 200L);
        assertThat(audienceCaptor.getValue())
                .extracting(TicketOrderAudience::getAudienceId, TicketOrderAudience::getNameSnapshot,
                        TicketOrderAudience::getIdNoHashSnapshot)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(301L, "甲", "hash-a"),
                        org.assertj.core.groups.Tuple.tuple(302L, "乙", "hash-b"));
    }

    @Test
    void planTransitionRowCountMismatchDoesNotMarkOrderRequestSuccessful() {
        TicketOrderRequest queued = queuedRequest();
        queued.setPurchasePlanId(90L);
        TicketOrderRequest processing = processingRequest();
        processing.setPurchasePlanId(90L);
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(ticketPurchasePlanAudienceMapper.selectByPlanIdAndSelectionType(90L, "SELECTED"))
                .thenReturn(List.of(new TicketPurchasePlanAudience(
                        1L, 90L, 301L, "SELECTED", 1, "甲", "hash-a", LocalDateTime.now())));
        when(ticketOrderAudienceMapper.insertBatch(anyList())).thenReturn(1);
        when(ticketPurchasePlanMapper.markOrderCreated(anyLong(), anyString(), anyLong(), anyString(), any()))
                .thenReturn(0);

        assertThatThrownBy(() -> consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1)))
                .isInstanceOf(ConsumerRetryableException.class);

        verify(orderRequestMapper, never()).markSuccess(anyLong(), anyLong());
    }

    @Test
    void planRemainsReconcilableWhenRedisCompensationIsNotConfirmed() {
        TicketOrderRequest queued = queuedRequest();
        queued.setPurchasePlanId(90L);
        TicketOrderRequest processing = processingRequest();
        processing.setPurchasePlanId(90L);
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(null);
        when(ticketCategoryMapper.existsShowSessionTicketCategoryRelation(1L, 1L, 2L)).thenReturn(false);
        when(orderRequestMapper.markFailed(10L, ErrorMessageConstant.SHOW_SESSION_TICKET_CATEGORY_NOT_MATCH))
                .thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, null, null, 1))
                .thenReturn(RedisStockReleaseResult.INVALID_QUANTITY);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(ticketPurchasePlanMapper, never()).markFailed(anyLong(), anyString(), anyString(), anyString(), any());
        verify(ticketPurchasePlanMapper).markReconciliationRequired(
                eq(90L), eq("REQ1"), eq("RECONCILIATION_REQUIRED"), anyString(), any());
    }

    @Test
    void consumerCreatesOrderOnlyWhenRelationIsValidAndUsesUnifiedTimeout() {
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        ArgumentCaptor<TicketOrder> orderCaptor = ArgumentCaptor.forClass(TicketOrder.class);
        verify(orderMapper).insert(orderCaptor.capture());
        TicketOrder createdOrder = orderCaptor.getValue();
        assertThat(createdOrder.getStatus()).isEqualTo(OrderStatusEnum.PENDING_PAYMENT.getCode());
        assertThat(createdOrder.getShowTitle()).isEqualTo("测试演唱会");
        assertThat(createdOrder.getTicketCategoryName()).isEqualTo("内场票");
        assertThat(createdOrder.getTicketPrice()).isEqualByComparingTo("880.00");
        assertThat(Duration.between(createdOrder.getCreatedAt(), createdOrder.getExpireTime()).toMinutes())
                .isEqualTo(OrderConstant.ORDER_TIMEOUT_MINUTES);
        ArgumentCaptor<OrderTimeoutMessage> timeoutCaptor = ArgumentCaptor.forClass(OrderTimeoutMessage.class);
        verify(orderTimeoutProducer).sendOrderTimeoutMessage(timeoutCaptor.capture());
        assertThat(timeoutCaptor.getValue().getOrderId()).isEqualTo(200L);
        assertThat(timeoutCaptor.getValue().getOrderNo()).isEqualTo(createdOrder.getOrderNo());
        assertThat(timeoutCaptor.getValue().getUserId()).isEqualTo(1L);
        assertThat(timeoutCaptor.getValue().getExpireTime()).isEqualTo(createdOrder.getExpireTime());
        assertThat(timeoutCaptor.getValue().getTraceId()).isEqualTo("order-timeout-200");
        verify(orderRequestMapper).markSuccess(10L, 200L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void consumeBatchUsesBatchSqlForValidMessages() {
        TicketOrderRequest firstRequest = processingRequest();
        TicketOrderRequest secondRequest = processingRequest("REQ2", 11L);
        when(orderRequestMapper.insertIgnoreBatch(anyList())).thenReturn(0);
        when(orderRequestMapper.tryMarkProcessingBatch(anyList(), any())).thenReturn(2);
        when(orderRequestMapper.selectProcessingByRequestIdsForUpdate(anyList(), any()))
                .thenReturn(List.of(firstRequest, secondRequest));
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStockBatch(anyList())).thenReturn(1);
        when(orderMapper.insertBatch(anyList())).thenAnswer(invocation -> {
            List<TicketOrder> orders = invocation.getArgument(0);
            orders.get(0).setId(201L);
            orders.get(1).setId(202L);
            return 2;
        });
        when(orderRequestMapper.markSuccessBatch(anyList())).thenReturn(2);

        consumer.consumeBatch(List.of(
                new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1),
                new AsyncCreateOrderMessage("REQ2", 1L, 1L, 1L, 2L, 1)
        ));

        ArgumentCaptor<List<StockDecreaseCommand>> stockCaptor = ArgumentCaptor.forClass(List.class);
        verify(ticketStockMapper).decreaseStockBatch(stockCaptor.capture());
        assertThat(stockCaptor.getValue()).hasSize(1);
        assertThat(stockCaptor.getValue().get(0).getTicketCategoryId()).isEqualTo(2L);
        assertThat(stockCaptor.getValue().get(0).getQuantity()).isEqualTo(2);

        ArgumentCaptor<List<TicketOrder>> orderCaptor = ArgumentCaptor.forClass(List.class);
        verify(orderMapper).insertBatch(orderCaptor.capture());
        assertThat(orderCaptor.getValue()).hasSize(2);
        assertThat(orderCaptor.getValue())
                .extracting(TicketOrder::getStatus)
                .containsOnly(OrderStatusEnum.PENDING_PAYMENT.getCode());
        verify(orderMapper, never()).insert(any(TicketOrder.class));

        ArgumentCaptor<List<OrderRequestSuccessBind>> bindCaptor = ArgumentCaptor.forClass(List.class);
        verify(orderRequestMapper).markSuccessBatch(bindCaptor.capture());
        assertThat(bindCaptor.getValue()).hasSize(2);
        assertThat(bindCaptor.getValue())
                .extracting(OrderRequestSuccessBind::getRequestDbId)
                .containsExactlyInAnyOrder(10L, 11L);
        assertThat(bindCaptor.getValue())
                .extracting(OrderRequestSuccessBind::getOrderId)
                .containsExactlyInAnyOrder(201L, 202L);
        verify(orderTimeoutProducer, times(2)).sendOrderTimeoutMessage(any(OrderTimeoutMessage.class));
    }

    @Test
    void consumeBatchFallsBackWhenBatchStockIsNotEnough() {
        TicketOrderRequest firstRequest = processingRequest();
        TicketOrderRequest secondRequest = processingRequest("REQ2", 11L);
        when(orderRequestMapper.insertIgnoreBatch(anyList())).thenReturn(0);
        when(orderRequestMapper.tryMarkProcessingBatch(anyList(), any())).thenReturn(2);
        when(orderRequestMapper.selectProcessingByRequestIdsForUpdate(anyList(), any()))
                .thenReturn(List.of(firstRequest, secondRequest));
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStockBatch(anyList())).thenReturn(0);

        assertThatThrownBy(() -> consumer.consumeBatch(List.of(
                new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1),
                new AsyncCreateOrderMessage("REQ2", 1L, 1L, 1L, 2L, 1)
        )))
                .isInstanceOf(ConsumerRetryableException.class)
                .hasMessageContaining("批量扣减普通库存失败");

        verify(orderMapper, never()).insertBatch(anyList());
        verify(orderRequestMapper, never()).markSuccessBatch(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void consumeBatchResolvesOrderIdsByOrderNoWhenDriverDoesNotReturnGeneratedKeys() {
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.insertIgnoreBatch(anyList())).thenReturn(0);
        when(orderRequestMapper.tryMarkProcessingBatch(anyList(), any())).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestIdsForUpdate(anyList(), any()))
                .thenReturn(List.of(processing));
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStockBatch(anyList())).thenReturn(1);
        when(orderMapper.insertBatch(anyList())).thenReturn(1);
        when(orderMapper.selectByOrderNos(anyList())).thenAnswer(invocation -> {
            List<String> orderNos = invocation.getArgument(0);
            TicketOrder savedOrder = new TicketOrder();
            savedOrder.setId(300L);
            savedOrder.setOrderNo(orderNos.get(0));
            return List.of(savedOrder);
        });
        when(orderRequestMapper.markSuccessBatch(anyList())).thenReturn(1);

        consumer.consumeBatch(List.of(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1)));

        ArgumentCaptor<List<OrderRequestSuccessBind>> bindCaptor = ArgumentCaptor.forClass(List.class);
        verify(orderRequestMapper).markSuccessBatch(bindCaptor.capture());
        assertThat(bindCaptor.getValue()).hasSize(1);
        assertThat(bindCaptor.getValue().get(0).getRequestDbId()).isEqualTo(10L);
        assertThat(bindCaptor.getValue().get(0).getOrderId()).isEqualTo(300L);
    }

    @Test
    void consumerSkipsRelationCheckWhenSnapshotExists() {
        consumer = consumerWithShowRelationCache();
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(showRelationCacheService, never()).existsPublishedRelation(anyLong(), anyLong(), anyLong());
        verify(ticketCategoryMapper, never()).existsShowSessionTicketCategoryRelation(anyLong(), anyLong(), anyLong());
        verify(orderMapper).insert(any(TicketOrder.class));
    }

    @Test
    void consumerReleasesInFlightAfterSuccessfulOrderCreation() {
        consumer = consumerWithInFlightService();
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(asyncOrderInFlightService).release("show:1:session:1", 2L);
    }

    @Test
    void consumerUsesOrderSnapshotCacheWhenAvailable() {
        consumer = consumerWithOrderSnapshotCache();
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(orderSnapshotCacheService.getPublishedSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderSnapshotCacheService).getPublishedSnapshot(1L, 1L, 2L);
        verify(ticketCategoryMapper, never()).selectOrderSnapshot(anyLong(), anyLong(), anyLong());
        verify(orderMapper).insert(any(TicketOrder.class));
    }

    @Test
    void consumerRejectsInvalidRelationAndRollsBackRedisPreDeductedStock() {
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.existsShowSessionTicketCategoryRelation(1L, 1L, 2L)).thenReturn(false);
        when(orderRequestMapper.markFailed(10L, ErrorMessageConstant.SHOW_SESSION_TICKET_CATEGORY_NOT_MATCH))
                .thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, null, null, 1))
                .thenReturn(RedisStockReleaseResult.SUCCESS);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(ticketStockMapper, never()).decreaseStock(anyLong(), anyInt());
        verify(orderMapper, never()).insert(any(TicketOrder.class));
        verify(stockLuaService).releasePreDeductedStock("REQ1", 2L, null, null, 1);
        verify(orderRequestMapper).markCompensated(anyLong(), any());
        verify(deadLetterMessageService).recordAsyncCreateOrderDeadLetter(
                any(),
                anyString(),
                anyString(),
                anyString(),
                any(),
                org.mockito.ArgumentMatchers.eq(ConsumerExceptionTypeEnum.BUSINESS_REJECT),
                org.mockito.ArgumentMatchers.eq(ErrorMessageConstant.SHOW_SESSION_TICKET_CATEGORY_NOT_MATCH)
        );
    }

    @Test
    void consumerRejectsMissingSnapshotAfterRelationFallback() {
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(null);
        when(ticketCategoryMapper.existsShowSessionTicketCategoryRelation(1L, 1L, 2L)).thenReturn(true);
        when(orderRequestMapper.markFailed(10L, ErrorMessageConstant.TICKET_CATEGORY_NOT_FOUND))
                .thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, null, null, 1))
                .thenReturn(RedisStockReleaseResult.SUCCESS);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(ticketStockMapper, never()).decreaseStock(anyLong(), anyInt());
        verify(orderMapper, never()).insert(any(TicketOrder.class));
        verify(deadLetterMessageService).recordAsyncCreateOrderDeadLetter(
                any(),
                anyString(),
                anyString(),
                anyString(),
                any(),
                org.mockito.ArgumentMatchers.eq(ConsumerExceptionTypeEnum.BUSINESS_REJECT),
                org.mockito.ArgumentMatchers.eq(ErrorMessageConstant.TICKET_CATEGORY_NOT_FOUND)
        );
    }

    @Test
    void consumerSkipsSuccessRequestAndDoesNotCreateDuplicateOrder() {
        TicketOrderRequest success = queuedRequest();
        success.setStatus(OrderRequestStatusEnum.SUCCESS.getCode());
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(success);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderRequestMapper, never()).tryMarkProcessing(anyString());
        verify(orderMapper, never()).insert(any());
    }

    @Test
    void consumerSkipsFailedRequestAndDoesNotCreateOrder() {
        TicketOrderRequest failed = queuedRequest();
        failed.setStatus(OrderRequestStatusEnum.FAILED.getCode());
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(failed);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderRequestMapper, never()).tryMarkProcessing(anyString());
        verify(orderMapper, never()).insert(any());
    }

    @Test
    void consumerReleasesRedisWhenMysqlStockIsNotEnough() {
        TicketOrderRequest queued = queuedRequest();
        TicketOrderRequest processing = processingRequest();
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(0);
        when(orderRequestMapper.markFailed(10L, "库存不足")).thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, null, null, 1))
                .thenReturn(RedisStockReleaseResult.SUCCESS);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderMapper, never()).insert(any(TicketOrder.class));
        verify(stockLuaService).releasePreDeductedStock("REQ1", 2L, null, null, 1);
    }

    @Test
    void consumerReleasesOriginalBucketWhenBucketMysqlStockIsNotEnough() {
        consumer = bucketEnabledConsumer();
        TicketOrderRequest queued = queuedRequest();
        queued.setStockBucketVersion(1);
        queued.setStockBucketNo(4);
        TicketOrderRequest processing = processingRequest();
        processing.setStockBucketVersion(1);
        processing.setStockBucketNo(4);
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(queued);
        when(orderRequestMapper.tryMarkProcessing("REQ1")).thenReturn(1);
        when(orderRequestMapper.selectProcessingByRequestId("REQ1")).thenReturn(processing);
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockBucketMapper.decreaseStockByVersion(2L, 1, 4, 1)).thenReturn(0);
        when(orderRequestMapper.markFailed(10L, "库存不足")).thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, 1, 4, 1))
                .thenReturn(RedisStockReleaseResult.SUCCESS);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderMapper, never()).insert(any(TicketOrder.class));
        verify(ticketStockMapper, never()).decreaseStock(anyLong(), anyInt());
        verify(stockLuaService).releasePreDeductedStock("REQ1", 2L, 1, 4, 1);
    }

    @Test
    void consumerCreatesMissingRequestFromMessageAndCreatesOrder() {
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(null);
        String[] insertedStatus = new String[1];
        when(orderRequestMapper.insertIgnore(any(TicketOrderRequest.class))).thenAnswer(invocation -> {
            TicketOrderRequest request = invocation.getArgument(0);
            insertedStatus[0] = request.getStatus();
            request.setId(10L);
            return 1;
        });
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        AsyncCreateOrderMessage message = new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1);
        message.setStockBucketVersion(1);
        message.setStockBucketNo(4);
        message.setRedisDeducted(true);
        message.setDeductedQuantity(1);
        message.setDeductedAt(LocalDateTime.now());
        message.setMessageId("MSGREQ1");
        consumer.consume(message);

        ArgumentCaptor<TicketOrderRequest> requestCaptor = ArgumentCaptor.forClass(TicketOrderRequest.class);
        verify(orderRequestMapper).insertIgnore(requestCaptor.capture());
        assertThat(insertedStatus[0]).isEqualTo(OrderRequestStatusEnum.PROCESSING.getCode());
        assertThat(requestCaptor.getValue().getStockBucketVersion()).isEqualTo(1);
        assertThat(requestCaptor.getValue().getStockBucketNo()).isEqualTo(4);
        assertThat(requestCaptor.getValue().getMessageId()).isEqualTo("MSGREQ1");
        verify(orderRequestMapper, never()).tryMarkProcessing(anyString());
        verify(orderMapper).insert(any(TicketOrder.class));
    }

    @Test
    void fastPipelineConsumerCreatesProcessingRequestBeforeSelectingExistingRequest() {
        AsyncOrderSubmitProperties asyncOrderSubmitProperties = new AsyncOrderSubmitProperties();
        asyncOrderSubmitProperties.setPersistRequestBeforePublish(false);
        ReflectionTestUtils.setField(consumer, "asyncOrderSubmitProperties", asyncOrderSubmitProperties);
        when(orderRequestMapper.insertIgnore(any(TicketOrderRequest.class))).thenAnswer(invocation -> {
            TicketOrderRequest request = invocation.getArgument(0);
            request.setId(10L);
            return 1;
        });
        when(userMapper.selectById(1L)).thenReturn(new UserAccount(1L, "tester", "13800000001", "encoded", "NORMAL", "USER", null, null));
        when(ticketCategoryMapper.selectOrderSnapshot(1L, 1L, 2L)).thenReturn(orderSnapshot());
        when(ticketStockMapper.decreaseStock(2L, 1)).thenReturn(1);
        when(orderMapper.insert(any(TicketOrder.class))).thenAnswer(invocation -> {
            TicketOrder order = invocation.getArgument(0);
            order.setId(200L);
            return 1;
        });
        when(orderRequestMapper.markSuccess(10L, 200L)).thenReturn(1);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderRequestMapper).insertIgnore(any(TicketOrderRequest.class));
        verify(orderRequestMapper, never()).selectByRequestId("REQ1");
        verify(orderRequestMapper, never()).tryMarkProcessing("REQ1");
        verify(orderMapper).insert(any(TicketOrder.class));
    }

    @Test
    void deadLetterUsesRocketMqTopicWhenRocketMqTransportIsActive() {
        AsyncOrderSubmitProperties properties = new AsyncOrderSubmitProperties();
        properties.setPublisherMode(AsyncOrderSubmitProperties.PUBLISHER_MODE_ROCKETMQ);
        properties.setRocketMqAsyncCreateOrderTopic("smart-ticket.async-order.create.rocket");
        ReflectionTestUtils.setField(consumer, "asyncOrderSubmitProperties", properties);
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(null);
        when(orderRequestMapper.insertIgnore(any(TicketOrderRequest.class))).thenReturn(0);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(deadLetterMessageService).recordAsyncCreateOrderDeadLetter(
                any(),
                eq("smart-ticket.async-order.create.rocket"),
                eq("smart-ticket.async-order.create.rocket"),
                anyString(),
                any(),
                eq(ConsumerExceptionTypeEnum.DATA_INCONSISTENCY),
                eq("异步下单请求补建失败")
        );
    }

    @Test
    void consumerRecordsDeadLetterWhenMissingRequestCannotBeCreated() {
        when(orderRequestMapper.selectByRequestId("REQ1")).thenReturn(null);
        when(orderRequestMapper.insertIgnore(any(TicketOrderRequest.class))).thenReturn(0);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(deadLetterMessageService).recordAsyncCreateOrderDeadLetter(
                any(),
                anyString(),
                anyString(),
                anyString(),
                any(),
                org.mockito.ArgumentMatchers.eq(ConsumerExceptionTypeEnum.DATA_INCONSISTENCY),
                org.mockito.ArgumentMatchers.eq("异步下单请求补建失败")
        );
        verify(orderRequestMapper, never()).tryMarkProcessing(anyString());
    }

    @Test
    void processingTimeoutMarksFailedAndCompensatesRedis() {
        TicketOrderRequest processing = processingRequest();
        processing.setProcessingAt(LocalDateTime.now().minusMinutes(5));
        when(orderRequestMapper.selectByRequestId("REQ1"))
                .thenReturn(processing)
                .thenReturn(processing);
        when(orderRequestMapper.markProcessingTimeout(10L, "异步下单请求PROCESSING超时，已进入人工处理")).thenReturn(1);
        when(orderRequestMapper.tryMarkCompensating(10L)).thenReturn(1);
        when(stockLuaService.releasePreDeductedStock("REQ1", 2L, null, null, 1))
                .thenReturn(RedisStockReleaseResult.SUCCESS);

        consumer.consume(new AsyncCreateOrderMessage("REQ1", 1L, 1L, 1L, 2L, 1));

        verify(orderRequestMapper).markProcessingTimeout(10L, "异步下单请求PROCESSING超时，已进入人工处理");
        verify(stockLuaService).releasePreDeductedStock("REQ1", 2L, null, null, 1);
        verify(deadLetterMessageService).recordAsyncCreateOrderDeadLetter(
                any(),
                anyString(),
                anyString(),
                anyString(),
                any(),
                org.mockito.ArgumentMatchers.eq(ConsumerExceptionTypeEnum.DATA_INCONSISTENCY),
                org.mockito.ArgumentMatchers.eq("异步下单请求PROCESSING超时，已进入人工处理")
        );
    }

    private TicketOrderRequest queuedRequest() {
        TicketOrderRequest request = baseRequest();
        request.setStatus(OrderRequestStatusEnum.QUEUED.getCode());
        request.setRedisDeducted(true);
        request.setDeductedQuantity(1);
        request.setCompensated(false);
        request.setCompensationStatus(CompensationStatusEnum.NONE.getCode());
        return request;
    }

    private TicketOrderRequest processingRequest() {
        return processingRequest("REQ1", 10L);
    }

    private TicketOrderRequest processingRequest(String requestId, Long id) {
        TicketOrderRequest request = baseRequest();
        request.setId(id);
        request.setRequestId(requestId);
        request.setStatus(OrderRequestStatusEnum.PROCESSING.getCode());
        request.setRedisDeducted(true);
        request.setDeductedQuantity(1);
        request.setProcessingAt(LocalDateTime.now());
        request.setCompensated(false);
        request.setCompensationStatus(CompensationStatusEnum.NONE.getCode());
        return request;
    }

    private TicketOrderRequest baseRequest() {
        TicketOrderRequest request = new TicketOrderRequest();
        request.setId(10L);
        request.setRequestId("REQ1");
        request.setUserId(1L);
        request.setShowId(1L);
        request.setSessionId(1L);
        request.setTicketCategoryId(2L);
        request.setQuantity(1);
        return request;
    }

    private AsyncCreateOrderConsumer bucketEnabledConsumer() {
        MqConsumerProperties mqConsumerProperties = new MqConsumerProperties();
        mqConsumerProperties.setProcessingTimeoutSeconds(120);
        StockBucketProperties stockBucketProperties = new StockBucketProperties();
        stockBucketProperties.setEnabled(true);
        stockBucketProperties.setDefaultBucketCount(10);
        return new AsyncCreateOrderConsumer(
                orderRequestMapper,
                orderMapper,
                userMapper,
                ticketCategoryMapper,
                ticketStockMapper,
                ticketStockBucketMapper,
                orderTimeoutProducer,
                stockLuaService,
                deadLetterMessageService,
                mqConsumerProperties,
                observabilityMetricsService,
                stockBucketProperties,
                null,
                null,
                null,
                asyncOrderInFlightService
        );
    }

    private AsyncCreateOrderConsumer consumerWithShowRelationCache() {
        MqConsumerProperties mqConsumerProperties = new MqConsumerProperties();
        mqConsumerProperties.setProcessingTimeoutSeconds(120);
        StockBucketProperties stockBucketProperties = new StockBucketProperties();
        stockBucketProperties.setEnabled(false);
        return new AsyncCreateOrderConsumer(
                orderRequestMapper,
                orderMapper,
                userMapper,
                ticketCategoryMapper,
                ticketStockMapper,
                ticketStockBucketMapper,
                orderTimeoutProducer,
                stockLuaService,
                deadLetterMessageService,
                mqConsumerProperties,
                observabilityMetricsService,
                stockBucketProperties,
                null,
                showRelationCacheService,
                null,
                asyncOrderInFlightService
        );
    }

    private AsyncCreateOrderConsumer consumerWithInFlightService() {
        MqConsumerProperties mqConsumerProperties = new MqConsumerProperties();
        mqConsumerProperties.setProcessingTimeoutSeconds(120);
        StockBucketProperties stockBucketProperties = new StockBucketProperties();
        stockBucketProperties.setEnabled(false);
        return new AsyncCreateOrderConsumer(
                orderRequestMapper,
                orderMapper,
                userMapper,
                ticketCategoryMapper,
                ticketStockMapper,
                ticketStockBucketMapper,
                orderTimeoutProducer,
                stockLuaService,
                deadLetterMessageService,
                mqConsumerProperties,
                observabilityMetricsService,
                stockBucketProperties,
                null,
                null,
                null,
                asyncOrderInFlightService
        );
    }

    private AsyncCreateOrderConsumer consumerWithOrderSnapshotCache() {
        MqConsumerProperties mqConsumerProperties = new MqConsumerProperties();
        mqConsumerProperties.setProcessingTimeoutSeconds(120);
        StockBucketProperties stockBucketProperties = new StockBucketProperties();
        stockBucketProperties.setEnabled(false);
        return new AsyncCreateOrderConsumer(
                orderRequestMapper,
                orderMapper,
                userMapper,
                ticketCategoryMapper,
                ticketStockMapper,
                ticketStockBucketMapper,
                orderTimeoutProducer,
                stockLuaService,
                deadLetterMessageService,
                mqConsumerProperties,
                observabilityMetricsService,
                stockBucketProperties,
                null,
                null,
                orderSnapshotCacheService,
                asyncOrderInFlightService
        );
    }

    private OrderSnapshot orderSnapshot() {
        OrderSnapshot snapshot = new OrderSnapshot();
        snapshot.setShowTitle("测试演唱会");
        snapshot.setSessionStartTime(LocalDateTime.of(2026, 6, 20, 19, 30));
        snapshot.setTicketCategoryName("内场票");
        snapshot.setTicketPrice(new BigDecimal("880.00"));
        return snapshot;
    }
}
