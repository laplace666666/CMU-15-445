//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  table_id_ = plan_->table_oid_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();  // 这里面存储了更新的数据，会造成内存泄漏
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (child_executor_ == nullptr) {
    return false;
  }
  Tuple update_tuple;
  RID update_rid;
  int count = 0;

  while (child_executor_->Next(&update_tuple, &update_rid)) {
    // 获取要更新的数据
    // 删除
    TupleMeta meta = table_info_->table_->GetTupleMeta(update_rid);
    meta.is_deleted_ = true;                                 // 不是物理删除，而是把他标记为删除
    table_info_->table_->UpdateTupleMeta(meta, update_rid);  // 更新回去，就完成了删除
    // 更新上下文
    std::vector<Value> values;
    for (auto &it : plan_->target_expressions_) {
      Value value = it->Evaluate(&update_tuple, table_info_->schema_);
      values.push_back(value);
    }
    Tuple u_tuple(values, &table_info_->schema_);
    TupleMeta meta_temp{};
    // 数据在后面一个参数里面
    std::optional<RID> insert_rid = table_info_->table_->InsertTuple(meta_temp, u_tuple);
    // 把更新的插入进去
    // 更新索引
    for (auto &index_info_tmp : index_list_) {
      // 先删掉再插入
      if (index_info_tmp != nullptr) {
        index_info_tmp->index_->DeleteEntry(update_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                      index_info_tmp->index_->GetKeyAttrs()),
                                            update_rid, exec_ctx_->GetTransaction());
        index_info_tmp->index_->InsertEntry(u_tuple.KeyFromTuple(table_info_->schema_, index_info_tmp->key_schema_,
                                                                 index_info_tmp->index_->GetKeyAttrs()),
                                            insert_rid.value(), exec_ctx_->GetTransaction());
      }
    }
    count++;  // 记录更新的行数
  }
  child_executor_ = nullptr;  // 子执行器为空，因为没有新增加一个字段
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  // 返回一个tuple记录，加入的结果，总共插入了多少数据
  return true;
}

}  // namespace bustub
