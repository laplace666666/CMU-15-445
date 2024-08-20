//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {
// 一个是上下文，一个是对应的计划节点，plan是存储信息的，可以使用列表初始化，减少一次函数调用
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // 初始化其他参数
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);  // 找到这张表，table_info里面存储了table_heap
  iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
  // 创建一个指向表头的迭代器，就是表迭代器
  // 所以就找到了这张表，并指向了表头
}

// 返回下一个tuple
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto [meta, tuple_] = iter_->GetTuple();
    // 判断是不是被删除了，记录在meta中
    if (meta.is_deleted_) {
      ++(*iter_);
      // 因为iter是一个指针指向的迭代器，解引用不是指向的迭代器指向的数据，而是指向的迭代器，其实就是指针指向指针
      continue;
    }
    *tuple = tuple_;
    *rid = iter_->GetRID();
    ++(*iter_);
    return true;  // 这里返回了所以就是找到的下一个有效的元祖
  }
  return false;
}

}  // namespace bustub
