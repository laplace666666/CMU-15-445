//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  is_accessible_.resize(num_frames + 1);  // +1是因为帧是从1开始编号的,帧的个数
  curr_size_ = 0;                         // 可以淘汰的数量
}
// 淘汰函数
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  *frame_id = 0;
  // 如果队列中就没有就不可能删除了
  if (history_map_.empty() && cache_map_.empty()) {
    return false;
  }
  // 优先淘汰队列，历史队列FIFO,优先淘汰末尾的数据，如果整个历史队列都找不到一个可以淘汰的就进入到缓存队列
  auto it = history_list_.end();
  while (it != history_list_.begin()) {
    it--;
    // 如果还没有被列为可驱逐
    if (!is_accessible_[*it]) {
      continue;
    }
    // 如果被列为了可驱逐
    *frame_id = *it;
    history_map_.erase(*it);
    use_count_[*it] = 0;
    curr_size_--;
    is_accessible_[*it] = false;
    history_list_.erase(it);
    return true;
  }
  // 如果不在历史队列中，在缓存队列中，就遵循LRU-K进行淘汰
  it = cache_list_.end();
  while (it != cache_list_.begin()) {
    it--;
    // 如果还没有被列为可驱逐
    if (!is_accessible_[*it]) {
      continue;
    }
    // 如果被列为了可驱逐
    *frame_id = *it;
    cache_map_.erase(*it);
    use_count_[*it] = 0;
    curr_size_--;
    is_accessible_[*it] = false;
    cache_list_.erase(it);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 等于k把编号从历史队列删除加到缓存队列中，大于k就刷新一次，把他加到缓存队列的最前面，小于k除了第一次创建时候把他加到表头其他不变
  // 在缓存队列中访问了就把他提到最前面，在历史队列中访问了只增加计数，不需要提取到最前面
  std::lock_guard<std::mutex> guard(latch_);
  // 边界异常,编号比帧的数量还要大就抛出异常
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  // 访问计数加一
  use_count_[frame_id]++;
  // 访问频次等于k就加到缓存队列中
  if (use_count_[frame_id] == k_) {
    // 如果在历史队列中
    if (history_map_.count(frame_id) != 0U) {
      auto it = history_map_[frame_id];  // 获取指向list对应元素的指针
      history_list_.erase(it);           // 历史队列删除这个点}
    }
    history_map_.erase(frame_id);  // 前面为什么要加一个判断呢，可能出于安全的考虑吧
    // 把这个添加到缓存队列中去
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (use_count_[frame_id] > k_) {
    // 大于k就重新刷新一下就行
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_map_.erase(frame_id);
    // 把他加到前面
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    // 自增之后还是小于k次
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}
// 设置为可淘汰的函数
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  // 边界异常,编号比帧的数量还要大就抛出异常
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  // 如果之前被设置成不能驱逐，现在想要设置成驱逐就数量相加
  if (!is_accessible_[frame_id] && set_evictable) {
    curr_size_++;  // 可以驱逐的数量
  }
  // 如果之前设置成可以驱逐，但是现在要求不能驱逐，数量就剪剪
  if (is_accessible_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_accessible_[frame_id] = set_evictable;
  // 这个api的作用就是设置是否可以被，同时更改被驱逐的数目，判断是为了避免重复计数
}

// 把一个帧直接删除掉,这个函数就是找到了就把他清除掉
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (!is_accessible_[frame_id]) {
    return;
  }
  if (frame_id > static_cast<int>(replacer_size_)) {
    return;
  }
  // 删除记录
  if (use_count_[frame_id] < k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  } else {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  }
  // 删除记录
  use_count_[frame_id] = 0;
  // 当前可用大小减少
  is_accessible_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub

/*
总的来说，RecordAccess实现了在访问数据的时候历史队列和缓存队列的维护
SetEvictable(frame_id_t frame_id, bool set_evictable)手动设置某个帧是否可以被驱逐
Evict(frame_id_t *frame_id)返回一个可以被驱逐的帧id并把他删除，基于规则
Remove只要在并且允许就把他删除，不管他的优先级。
这里指示操作了index，或者说是页号
*/
