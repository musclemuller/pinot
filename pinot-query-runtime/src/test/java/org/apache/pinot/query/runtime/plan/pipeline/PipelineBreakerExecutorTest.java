/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.plan.pipeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.when;


public class PipelineBreakerExecutorTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final String MAILBOX_ID_1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
  private static final String MAILBOX_ID_2 = MailboxIdUtils.toMailboxId(0, 2, 0, 0, 0);

  private final ExecutorService _executor = Executors.newCachedThreadPool();
  private final OpChainSchedulerService _scheduler = new OpChainSchedulerService(_executor);
  private final MailboxInfos _mailboxInfos =
      new SharedMailboxInfos(new MailboxInfo("localhost", 123, ImmutableList.of(0)));
  private final WorkerMetadata _workerMetadata =
      new WorkerMetadata(0, ImmutableMap.of(1, _mailboxInfos, 2, _mailboxInfos), ImmutableMap.of());
  private final StageMetadata _stageMetadata =
      new StageMetadata(0, ImmutableList.of(_workerMetadata), ImmutableMap.of());

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;

  @BeforeMethod
  public void setUpMethod() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(123);

    when(_mailbox1.getId()).thenReturn("mailbox1");
    when(_mailbox1.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailbox2.getId()).thenReturn("mailbox2");
    when(_mailbox2.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @Nullable
  public static PipelineBreakerResult executePipelineBreakers(OpChainSchedulerService scheduler,
      MailboxService mailboxService, WorkerMetadata workerMetadata, StagePlan stagePlan,
      Map<String, String> opChainMetadata, long requestId, long deadlineMs) {
    return PipelineBreakerExecutor.executePipelineBreakers(scheduler, mailboxService, workerMetadata, stagePlan,
        opChainMetadata, requestId, deadlineMs, deadlineMs, null, true);
  }

  @AfterClass
  public void tearDown() {
    ExecutorServiceUtils.close(_executor);
  }

  @Test
  public void shouldReturnBlocksUponNormalOperation()
      throws IOException {
    MailboxReceiveNode mailboxReceiveNode = getPBReceiveNode(1);
    StagePlan stagePlan = new StagePlan(mailboxReceiveNode, _stageMetadata);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2),
        OperatorTestUtil.eosWithStats(OperatorTestUtil.getDummyStats(1).serialize()));

    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, Long.MAX_VALUE);

    // then
    // should have single PB result, receive 2 data blocks, EOS block shouldn't be included
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertNull(pipelineBreakerResult.getErrorBlock());
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 1);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().values().iterator().next().size(), 2);

    // should collect stats from previous stage here
    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats());
    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats().getUpstreamStageStats(1),
        "Stats for stage 1 should be sent");
  }

  @Test
  public void shouldWorkWithMultiplePBNodeUponNormalOperation()
      throws IOException {
    MailboxReceiveNode mailboxReceiveNode1 = getPBReceiveNode(1);
    MailboxReceiveNode mailboxReceiveNode2 = getPBReceiveNode(2);
    JoinNode joinNode =
        new JoinNode(0, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(mailboxReceiveNode1, mailboxReceiveNode2),
            JoinRelType.INNER, List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH);
    StagePlan stagePlan = new StagePlan(joinNode, _stageMetadata);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.eosWithStats(OperatorTestUtil.getDummyStats(1).serialize()));
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2),
        OperatorTestUtil.eosWithStats(OperatorTestUtil.getDummyStats(2).serialize()));

    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, Long.MAX_VALUE);

    // then
    // should have two PB result, receive 2 data blocks, one each, EOS block shouldn't be included
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertNull(pipelineBreakerResult.getErrorBlock());
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 2);
    Iterator<List<MseBlock>> it = pipelineBreakerResult.getResultMap().values().iterator();
    Assert.assertEquals(it.next().size(), 1);
    Assert.assertEquals(it.next().size(), 1);
    Assert.assertFalse(it.hasNext());

    // should collect stats from previous stage here
    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats());
    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats().getUpstreamStageStats(1),
        "Stats for stage 1 should be sent");
    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats().getUpstreamStageStats(2),
        "Stats for stage 2 should be sent");
  }

  @Test
  public void shouldReturnEmptyBlockWhenPBExecuteWithIncorrectMailboxNode() {
    MailboxReceiveNode incorrectlyConfiguredMailboxNode = getPBReceiveNode(3);
    StagePlan stagePlan = new StagePlan(incorrectlyConfiguredMailboxNode, _stageMetadata);

    // when
    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, Long.MAX_VALUE);

    // then
    // should return empty block list
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertNull(pipelineBreakerResult.getErrorBlock());
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 1);
    List<MseBlock> resultBlocks = pipelineBreakerResult.getResultMap().values().iterator().next();
    Assert.assertEquals(resultBlocks.size(), 0);

    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats());
  }

  @Test
  public void shouldReturnErrorBlocksFailureWhenPBTimeout() {
    MailboxReceiveNode mailboxReceiveNode = getPBReceiveNode(1);
    StagePlan stagePlan = new StagePlan(mailboxReceiveNode, _stageMetadata);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    CountDownLatch latch = new CountDownLatch(1);
    when(_mailbox1.poll()).thenAnswer(invocation -> {
      latch.await();
      return SuccessMseBlock.INSTANCE;
    });

    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, System.currentTimeMillis() + 100);

    // then
    // should contain only failure error blocks
    Assert.assertNotNull(pipelineBreakerResult);
    MseBlock errorBlock = pipelineBreakerResult.getErrorBlock();
    Assert.assertNotNull(errorBlock);
    Assert.assertTrue(errorBlock.isError());

    latch.countDown();
  }

  @Test
  public void shouldReturnWhenAnyPBReturnsEmpty() {
    MailboxReceiveNode mailboxReceiveNode1 = getPBReceiveNode(1);
    MailboxReceiveNode incorrectlyConfiguredMailboxNode = getPBReceiveNode(3);
    JoinNode joinNode = new JoinNode(0, DATA_SCHEMA, PlanNode.NodeHint.EMPTY,
        List.of(mailboxReceiveNode1, incorrectlyConfiguredMailboxNode), JoinRelType.INNER, List.of(0), List.of(0),
        List.of(), JoinNode.JoinStrategy.HASH);
    StagePlan stagePlan = new StagePlan(joinNode, _stageMetadata);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.eosWithStats(List.of()));
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2),
        OperatorTestUtil.eosWithStats(List.of()));

    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, Long.MAX_VALUE);

    // then
    // should pass when one PB returns result, the other returns empty.
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 2);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().get(0).size(), 1);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().get(1).size(), 0);

    Assert.assertNotNull(pipelineBreakerResult.getStageQueryStats());
  }

  @Test
  public void shouldReturnErrorBlocksWhenReceivedErrorFromSender() {
    MailboxReceiveNode mailboxReceiveNode1 = getPBReceiveNode(1);
    MailboxReceiveNode incorrectlyConfiguredMailboxNode = getPBReceiveNode(2);
    JoinNode joinNode = new JoinNode(0, DATA_SCHEMA, PlanNode.NodeHint.EMPTY,
        List.of(mailboxReceiveNode1, incorrectlyConfiguredMailboxNode), JoinRelType.INNER, List.of(0), List.of(0),
        List.of(), JoinNode.JoinStrategy.HASH);
    StagePlan stagePlan = new StagePlan(joinNode, _stageMetadata);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.errorWithStats(new RuntimeException("ERROR ON 1"), List.of()));
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2),
        OperatorTestUtil.eosWithStats(List.of()));

    PipelineBreakerResult pipelineBreakerResult =
        executePipelineBreakers(_scheduler, _mailboxService, _workerMetadata, stagePlan,
            ImmutableMap.of(), 0, Long.MAX_VALUE);

    // then
    // should fail even if one of the 2 PB doesn't contain error block from sender.
    Assert.assertNotNull(pipelineBreakerResult);
    MseBlock errorBlock = pipelineBreakerResult.getErrorBlock();
    Assert.assertNotNull(errorBlock);
    Assert.assertTrue(errorBlock.isEos() && ((MseBlock.Eos) errorBlock).isError());
  }

  private static MailboxReceiveNode getPBReceiveNode(int senderStageId) {
    return new MailboxReceiveNode(0, DATA_SCHEMA, senderStageId, PinotRelExchangeType.PIPELINE_BREAKER,
        RelDistribution.Type.SINGLETON, null, null, false, false, null);
  }
}
