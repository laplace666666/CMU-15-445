//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  // 先申请一个物理页号，下面要考虑存储在缓冲池的那个页当中
  int new_page_id = AllocatePage();
  frame_id_t frame_id;
  // 先从空闲列表中申请
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 空闲列表为空就淘汰一个页面
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 这时候删除的缓冲池页号就存储在了frame_id
    // 脏读就要重新写会内存，上面的只是把id删除了，但是物理内存还没有操作
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());  // pages_[frame_id].GetPageId()能够返回对应的磁盘页面号
  }
  // 建立物理页到实际页的映射
  page_table_[new_page_id] = frame_id;
  // 这里创建了一个指针指向新创建的这个页，因为最终返回是一个指针
  auto &current_page = pages_[frame_id];
  // 这个是这个磁盘页的属性，记录其存储的物理内存页号是多少
  current_page.page_id_ = new_page_id;
  current_page.ResetMemory();  // 清空datau数据
  current_page.is_dirty_ = false;
  current_page.pin_count_ = 1;  // pin_count此页面的固定次数，当前正在使用所以标为1，后面要手动释放他

  // LRU-K对页面进行管理
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);  // 把这个页面设置成不可驱逐

  *page_id = new_page_id;  // 返回创建的缓冲池号
  return &current_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // 判断是不是在缓冲池中
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    // 确实只有在重复访问现有磁盘的时候才++
    pages_[frame_id].pin_count_++;  // 说明正在使用，这个在取完数据之后就使用unpin释放掉了,这样就不会删除这个页面了，
    // 更新RLU-K
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];  // 要返回一直指针
  }
  // 在缓冲池当中没有找到的话，就要去磁盘中读取
  if (!free_list_.empty()) {
    // 先从空闲链表
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 需要替换
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 这时候如果淘汰成功，就把淘汰的页号存储在了frame_id中，并且u删除了RLU中所有的记录
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  page_table_[page_id] = frame_id;
  // 缓存区页面的元属性
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;  // 新创建的时候就直接赋值为1

  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}
// 这个就是给指定页面接触固定，因为在其他文件中，都会给这个页面在RLU中设置成不可驱逐，没有地方修改，这里就可以修改
// 对一个页操作完之后就要unpin
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // 是个是看到底物理页面有没有写入到磁盘当中
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  //  不能把原来脏的状态取消，// 保留原来的脏状态，如果新传入的 `is_dirty` 为真，则将页面标记为脏
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);  // 没有人再用的时候就会使用RLU规则删除了
    }
    return true;
  }
  return false;
}
// 就是把这个页面刷到磁盘上
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  const std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}
// 从磁盘中删除 页面，给定物理页面号
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  // pin_count != 0 代表是否正在被使用
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;  // 代表还没有存储数据
  pages_[frame_id].ResetMemory();               // 清空datau数据
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);  // 释放这个内存
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();       // 读共享锁
  return {this, page};  // 调用另外的一种构造函数
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();       // 读共享锁
  return {this, page};  // 调用另外的一种构造函数
}
// 这样读和写的时候就调用这两个接口就行了，并且还加了锁，还可以用类自己创建的Drop进行释放

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
/*
创建一个新页：
先申请一个物理页号，
再申请一个缓冲池页号，可以从缓冲池的空闲列表中拿，如果都不空闲就使用LRU-k算法淘汰一个缓冲池页面，
如果要淘汰的页面被修改了（脏页）就需要重新写入磁盘当中之后再在上面写，
需要更新物理页和缓冲池页的映射，
更新LKU
更新页的原属性
缓冲区的容量就那么大，如果某个页为空就把他放到空闲列表中（free_list_）中
page_id_可以是非法的。
磁盘中的页和缓冲区的页的大小不一样，缓冲区的页要大一点，还要记录一些页的属性，比如说缓冲区的页和物理页的对应关系、是否已经被更改，以及是否被使用等等。。。
*/
