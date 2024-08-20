//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)),  // 索引信息，按照某个索引扫描
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      index_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      iter_(index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != index_->GetEndIterator()) {
    // 迭代器中放的是key-value数据
    *rid = (*iter_).second;  // 对应的RID
    auto [meta, tuple_temp] = table_info_->table_->GetTuple(*rid);
    // 有删除的元祖需要跳过
    if (meta.is_deleted_) {
      ++(iter_);
      continue;
    }
    *tuple = tuple_temp;
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
