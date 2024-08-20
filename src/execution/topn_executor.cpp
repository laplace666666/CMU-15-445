#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // 初始化子执行器，准备从子执行器中提取元组。
  child_executor_->Init();

  Tuple produce_tuple;  // 用于存储从子执行器获取的元组。
  RID produce_rid;      // 用于存储与元组对应的行标识符（Record ID）。

  // 定义一个比较器，用于根据排序规则比较两个元组。，定义优先顺序
  auto comp = [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    bool flag = false;  // 用于标记比较结果。

    // 遍历排序规则，每个规则指定一个排序字段和排序顺序（ASC/DESC）。
    for (auto &it : plan_->order_bys_) {
      // 计算两个元组在当前排序字段上的值。
      const Value left_value = it.second->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
      const Value right_value = it.second->Evaluate(&right_tuple, child_executor_->GetOutputSchema());

      // 比较两个值是否相等。
      bool is_equal = left_value.CompareEquals(right_value) == CmpBool::CmpTrue;

      if (!is_equal) {
        // 如果不相等，根据排序顺序决定 flag 的值。
        bool is_less_than = left_value.CompareLessThan(right_value) == CmpBool::CmpTrue;

        if (it.first == OrderByType::ASC || it.first == OrderByType::DEFAULT) {
          // 如果排序顺序是升序（ASC）或默认（DEFAULT），则 flag 为 true 如果 left_value 小于 right_value。
          flag = is_less_than;
        } else if (it.first == OrderByType::DESC) {
          // 如果排序顺序是降序（DESC），则 flag 为 true 如果 left_value 大于 right_value。
          flag = !is_less_than;
        } else {
          // 如果出现了未知的排序类型，断言失败（一般不会到达这里）。
          BUSTUB_ASSERT(true, "not enter here!");
        }
        return flag;  // 返回比较结果。
      }
    }
    return flag;  // 如果所有排序字段的值都相等，flag 保持 false（即两个元组相等）。
  };

  // 使用自定义的比较器 comp 初始化一个优先队列，用于存储当前的 Top-N 元组。
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);

  // 从子执行器中提取所有元组，并维护一个大小为 N 的优先队列。
  while (child_executor_->Next(&produce_tuple, &produce_rid)) {
    if (pq.size() < this->plan_->n_) {
      // 如果优先队列中的元素少于 N 个，直接将新元组插入队列。
      pq.push(produce_tuple);
    } else {
      // 如果优先队列已满，只有当新元组比队列中最大的元组更小（根据比较器）时，才插入新元组。
      if (comp(produce_tuple, pq.top())) {
        pq.pop();                // 移除队列中的最大元组。
        pq.push(produce_tuple);  // 插入新元组。
      }
    }
  }

  // 将优先队列中的元组按顺序提取到输出列表中，并按相反顺序排列（从小到大）。
  while (!pq.empty()) {
    out_puts_.push_back(pq.top());
    pq.pop();
  }
  std::reverse(out_puts_.begin(), out_puts_.end());  // 逆转列表，使最小的元素在前。

  // 初始化输出迭代器，用于逐个返回结果。
  iterator_ = out_puts_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == out_puts_.end()) {
    return false;
  }
  *tuple = *iterator_;
  iterator_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return out_puts_.size(); };

}  // namespace bustub
