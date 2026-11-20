# 恢复与观测

备份必须调用 archives.backup，不能只拷贝活动主文件。backup 与 restore 都不覆盖存在目标；恢复使用新路径，先校验来源 SHA-256，再打开新句柄。主库恢复生成新身份，副本恢复绑定来源。恢复清理原机保留点与消费者；消费方重新注册时须根据已恢复的 floor/head 判断是否需要重建。

diagnostics.inspect(deep=True) 检查 SQLite 完整性、外键、提交连续性、当前索引与文档投影；fingerprint 比较指定读视图的键、文档版本、内容及到期事实。前者不能证明所有业务数据正确，后者相同也不能代替模式与索引定义比对。checkpoint 返回 SQLite 的 busy、log、checkpointed 信息，并不承诺截断 WAL。

## 增量一致性校验

`Database.verify_peer(peer, collections=None, start=None, end=None, ...)` 比较两个句柄的文档内容，用于主库与副本之间的定位式核验。全库指纹只能说明“不同”，本方法按集合与键区间逐键给出可定位结果。

- 读修订协商。两侧各自持续产生提交时，方法取两侧仍可读修订的交集（`max(两个 floor) .. min(两个 head)`），在该修订上同时建立两侧读快照，作为唯一比较基点；协商完成后仍可继续提交，不影响已固定的读视图。可显式传入 `revision`，但必须落在交集内，否则以 `SnapshotTooOld` 拒绝（低于可比 floor、高于可比 ceiling 或交集为空）。返回结果中的 `revision` 是实际比较基点，`source_head`/`peer_head` 记录各自的最新水位，`revision_heads_equal` 只描述水位关系。
- 定位结果。`differences` 逐键给出 `missing_on_peer`（源有副本无）、`extra_on_peer`（副本有源无）、`content_mismatch`（文档或到期事实不同）与 `deletion_state_mismatch`（一侧墓碑一侧不存在），并附两侧 `state`/`revision`/内容摘要；`include_documents=True` 时还带完整文档。`difference_counts` 按类型汇总本页数量，`scope` 回显集合与键区间。
- 回收范围。`unverifiable_ranges` 列出因历史回收而无法比较的修订区间及原因（每侧 floor 之前的历史）；比较基点本身不受影响，因为 floor 保证每个键的基准版本仍在。两侧可读修订无交集时 `comparable` 为假、`content_equal` 为 `null`、`evidence` 为 `none`，`decision` 直接给出 `rebuild`；分页未完成时 `content_equal` 同样为 `null`，`evidence` 为 `partial_scoped_content_comparison`。
- 版本相同不等于内容相同。返回结构固定带有 `revision_identity_used_as_content_proof: false` 与 `heads_equal_but_content_differs`；只有 `verified_equal`（区间内每条键都在比较修订上核对过且无差异）才代表内容一致，`consistent` 另外要求目录模式与索引定义一致。
- 与追赶事实关联。`catch_up` 给出 `relation`（`peer_follows_source`/`source_follows_peer`/`same_identity`/`assumed_shared`）、`outstanding_revisions`、`unservable_gap`、`catalog_behind` 与 `can_catch_up`；`catch_up_effect` 进一步读取上游未交付提交，逐键标出 `repaired_by_catch_up`（会被后续提交覆盖）与 `kept_after_catch_up`（追赶后仍然错误）。跨页运行时累计全部页的差异键；观测到的差异键超过上限（64）时 `coverage_complete` 为假，`resolves_differences` 不再为真，调用方应缩小键区间重试。
- 调用方选择。`decision.action` 为 `none`、`catch_up`、`rebuild` 或 `resume`：内容一致但有未交付提交或目录差异时为 `catch_up`；差异全部会被未交付提交覆盖时为 `catch_up`；其余差异、追随者领先上游、无共同可读修订或目录差异无法随包传播时为 `rebuild`。`remediation` 并列两种恢复途径：`catch_up` 保留副本身份，并列出 `repaired_keys`（将被覆盖修复）与 `remaining_keys`（追赶后仍错误）；`rebuild` 以新备份重建副本（`Archives.restore(..., follower=True)`）并能修复全部内容差异。
- 分页与大区间。`limit` 限制单次检查的键数；结果 `has_more` 为真时把 `continue_token` 作为 `position` 回传，即可在固定比较修订与固定集合范围上续查。`run` 跨页累计 `pages`、`examined` 与 `differences`，末页的 `verified_equal`、`content_equal` 与 `decision` 覆盖整轮而非仅最后一页；`differences` 仍只列当前页条目，需要全量清单请放宽 `limit`。token 绑定数据库身份、peer、读修订、集合范围与键区间，被换用或篡改的 token 以 `InvalidCursor` 拒绝。
- 关系判定以角色为准。副本恢复保留上游 `database_id`，因此仅凭身份无法区分同库第二句柄与跟随者；实现依据 `mode`/`upstream` 判定方向。既非同一身份、也非同一复制链的句柄必须显式传 `assume_shared_history=True`，否则拒绝比较。

跨上游核验请让源库作为调用方；副本调用会得到 `source_follows_peer`（即“当前库落后于对端”），方向同样明确。

examples.observations 的 contention 会让独立连接暂持 IMMEDIATE 锁，记录第二句柄的 busy 超时及释放后的成功；retention_pressure 记录读保留点和消费者分别阻止水位推进的现象。它们用独立临时文件，不接触生产路径。观察包含真实修订和异常，适合复核运行边界。

关闭所有调用方句柄再移动数据目录；不支持共享网络盘或多主写入。在空间紧张时先核实 pin/消费者，推进已完成的确认后再回收；回收不自动缩小文件，物理容量由 SQLite 空闲页复用。到期清理只在显式调用时执行，不能把墙钟到期等同于读视图变动。
