# Graceful Failover 测试用例文档

## 测试目标
实现 100% 代码覆盖率，覆盖所有状态转换、错误处理和边界情况。

## 测试场景分类

### 1. 正常流程测试 (Happy Path)

#### 1.1 基本 Failover 流程
- **场景**: Master 成功将控制权转移给 Slave
- **验证点**:
  - 状态转换：none -> started -> check_slave -> pause_write -> wait_sync -> switching -> success
  - 写阻塞在 pause/wait_sync/switching 状态生效
  - 成功后所有 slot 被标记为 migrated，返回 MOVED
  - Slave 成功接收 TAKEOVER 命令

#### 1.2 带超时参数的 Failover
- **场景**: 使用自定义 timeout 参数
- **验证点**:
  - 默认 timeout (1000ms) 生效
  - 自定义 timeout 正确应用
  - 超时计算正确

#### 1.3 无密码认证的 Failover
- **场景**: 集群未配置 requirepass
- **验证点**:
  - 跳过 AUTH 步骤
  - 直接发送 TAKEOVER 命令

#### 1.4 有密码认证的 Failover
- **场景**: 集群配置了 requirepass
- **验证点**:
  - 先发送 AUTH 命令
  - AUTH 成功后发送 TAKEOVER
  - 密码正确时成功

### 2. 失败场景测试 (Failure Cases)

#### 2.1 Slave 节点不存在
- **场景**: 指定的 slave_node_id 不在集群中
- **验证点**:
  - 立即失败，状态变为 failed
  - 返回错误信息 "Slave node not found in cluster"

#### 2.2 Slave 未连接
- **场景**: Slave 节点存在但未建立复制连接
- **验证点**:
  - checkSlaveStatus 失败
  - 状态变为 failed
  - 错误信息 "Slave not connected or not syncing"

#### 2.3 Slave 未同步
- **场景**: Slave 已连接但未开始同步
- **验证点**:
  - GetSlaveReplicationOffset 失败
  - 状态变为 failed

#### 2.4 Slave Lag 检查失败 - 同步速度太慢
- **场景**: Slave 同步速度 <= 0.1 bytes/s
- **验证点**:
  - checkSlaveLag 失败
  - 状态变为 failed
  - 错误信息包含 "Slave is not replicating"

#### 2.5 Slave Lag 检查失败 - 预估时间超时
- **场景**: 预估的 catchup 时间 > 剩余 timeout
- **验证点**:
  - checkSlaveLag 失败
  - 状态变为 failed
  - 错误信息包含 "Estimated catchup time"

#### 2.6 等待同步超时
- **场景**: waitReplicationSync 超过 timeout_ms_
- **验证点**:
  - waitReplicationSync 失败
  - 状态变为 failed
  - 错误信息 "Timeout waiting for replication sync"

#### 2.7 连接 Slave 失败
- **场景**: 无法连接到 Slave 节点
- **验证点**:
  - sendTakeoverCmd 失败
  - 状态变为 failed
  - 错误信息 "Failed to connect to slave"

#### 2.8 AUTH 失败 - 密码错误
- **场景**: requirepass 配置但密码错误
- **验证点**:
  - AUTH 命令失败
  - 状态变为 failed
  - 错误信息 "AUTH failed"

#### 2.9 TAKEOVER 命令失败
- **场景**: Slave 返回非 OK 响应
- **验证点**:
  - sendTakeoverCmd 失败
  - 状态变为 failed
  - 错误信息 "TAKEOVER failed"

### 3. 并发和边界测试 (Concurrency & Edge Cases)

#### 3.1 重复发起 Failover
- **场景**: 在已有 failover 进行中时再次发起
- **验证点**:
  - Run() 返回错误
  - 错误信息 "Failover is already in progress"
  - 原有 failover 继续执行

#### 3.2 从 Failed 状态重新开始
- **场景**: 失败后重新发起 failover
- **验证点**:
  - 允许从 failed 状态重新开始
  - 新的 failover 可以正常执行

#### 3.3 写请求在 Failover 期间的行为
- **场景**: 在 pause/wait_sync/switching 状态发送写请求
- **验证点**:
  - 写请求返回 TRYAGAIN
  - 错误信息 "Failover in progress"
  - 读请求不受影响

#### 3.4 读请求在 Failover 期间的行为
- **场景**: 在 failover 期间发送读请求
- **验证点**:
  - 读请求正常处理
  - 不受写阻塞影响

#### 3.5 不同 Timeout 值测试
- **场景**: 测试各种 timeout 值
- **验证点**:
  - timeout = 0 (最小有效值)
  - timeout = 100 (小值)
  - timeout = 10000 (大值)
  - timeout < 0 (无效值，应返回错误)

### 4. 状态查询测试 (State Query)

#### 4.1 CLUSTER INFO 输出状态
- **场景**: 查询 failover 状态信息
- **验证点**:
  - none 状态正确显示
  - started 状态正确显示
  - check_slave 状态正确显示
  - pause_write 状态正确显示
  - wait_sync 状态正确显示
  - switching 状态正确显示
  - success 状态正确显示
  - failed 状态正确显示

### 5. 集成测试 (Integration)

#### 5.1 SETNODES 重置状态
- **场景**: Controller 更新拓扑后重置 failover 状态
- **验证点**:
  - SetClusterNodes 调用 ResetFailoverState
  - 状态重置为 none

#### 5.2 TAKEOVER 命令处理
- **场景**: Slave 接收 TAKEOVER 命令
- **验证点**:
  - OnTakeOver 正确执行
  - imported_slots_ 正确设置
  - 返回 OK

#### 5.3 Failover 成功后 Slot 重定向
- **场景**: Failover 成功后访问原 Master 的 slot
- **验证点**:
  - 所有 slot 返回 MOVED
  - MOVED 指向新 Master (slave)
  - 新 Master 可以正常处理请求

#### 5.4 数据一致性验证
- **场景**: Failover 前后数据一致性
- **验证点**:
  - 所有数据成功复制到新 Master
  - 无数据丢失
  - 新 Master 可以正常读写

### 6. 性能测试 (Performance)

#### 6.1 大 Lag 场景
- **场景**: Slave 有较大 lag，但能在 timeout 内 catch up
- **验证点**:
  - 正确计算预估时间
  - 成功完成 failover

#### 6.2 快速同步场景
- **场景**: Slave 几乎实时同步
- **验证点**:
  - 快速进入 pause 状态
  - 快速完成同步等待
  - 总耗时短

## 代码覆盖率目标

### 需要覆盖的函数
1. `ClusterFailover::Run()` - 100%
2. `ClusterFailover::IsWriteForbidden()` - 100%
3. `ClusterFailover::GetSlaveNodeId()` - 100%
4. `ClusterFailover::GetFailoverInfo()` - 100% (所有状态分支)
5. `ClusterFailover::ResetFailoverState()` - 100%
6. `ClusterFailover::loop()` - 100%
7. `ClusterFailover::runFailoverProcess()` - 100%
8. `ClusterFailover::checkSlaveStatus()` - 100%
9. `ClusterFailover::checkSlaveLag()` - 100% (所有分支)
10. `ClusterFailover::waitReplicationSync()` - 100%
11. `ClusterFailover::sendTakeoverCmd()` - 100% (有/无密码分支)
12. `ClusterFailover::abortFailover()` - 100%

### 需要覆盖的状态转换
- kNone -> kStarted
- kStarted -> kCheck
- kCheck -> kPause (成功)
- kCheck -> kFailed (失败)
- kPause -> kSyncWait
- kSyncWait -> kSwitch (成功)
- kSyncWait -> kFailed (超时)
- kSwitch -> kSuccess (成功)
- kSwitch -> kFailed (失败)
- kFailed -> kStarted (重新开始)

### 需要覆盖的错误路径
- Slave 节点不存在
- Slave 未连接
- Slave 未同步
- Lag 检查失败 (速度太慢)
- Lag 检查失败 (预估时间超时)
- 等待同步超时
- 连接失败
- AUTH 失败
- TAKEOVER 失败

