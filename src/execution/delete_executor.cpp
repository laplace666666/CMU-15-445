//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple delete_tuple{};
  RID delete_rid{};
  int delete_num = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    // 先标记删除
    TupleMeta meta = table_info_->table_->GetTupleMeta(delete_rid);
    meta.is_deleted_ = true;
    // 重新写回去
    table_info_->table_->UpdateTupleMeta(meta, delete_rid);  // 才真正的删除了
    // 删除索引
    auto index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index_info : index_infos_) {
      index_info->index_->DeleteEntry(
          delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          delete_rid, exec_ctx_->GetTransaction());
    }
    delete_num++;
  }
  // 把删除的数量返回给调用者，创建一个新的tuple
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, delete_num);
  Tuple tuple_temp{values, &plan_->OutputSchema()};
  *tuple = tuple_temp;
  is_end_ = true;
  return true;
}

}  // namespace bustub
