//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}
// 一次调用全部插入完成
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int insert_num = 0;  // 记录插入的数据
  Tuple insert_tuple{};
  RID insert_rid{};
  // 以此做插入
  TupleMeta tupleMeta{};
  // 从child里面拿数据
  while (child_->Next(&insert_tuple, &insert_rid)) {
    tupleMeta.is_deleted_ = false;
    tupleMeta.insert_txn_id_ = INVALID_TXN_ID;
    tupleMeta.delete_txn_id_ = INVALID_TXN_ID;
    // 插入返回一个id
    std::optional<RID> new_rid = table_info_->table_->InsertTuple(tupleMeta, insert_tuple, exec_ctx_->GetLockManager(),
                                                                  exec_ctx_->GetTransaction(), plan_->table_oid_);
    if (!new_rid) {
      continue;
    }
    // 更新tuple的索引
    // 首先要知道有哪些索引
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      // 把tuple加入到索引中
      // 索引需要的key，因为索引的关键字是不一样的
      index_info->index_->InsertEntry(
          insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_rid.value(), exec_ctx_->GetTransaction());
    }
    insert_num++;
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, insert_num);
  Tuple tuple_temp{values, &plan_->OutputSchema()};
  *tuple = tuple_temp;
  is_end_ = true;
  return true;
}

}  // namespace bustub
