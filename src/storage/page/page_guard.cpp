#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
// 总共有三个属性，一个是缓冲池，缓冲池中的那个页面指针，这个页面是否脏了
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // 先把自己的清空
  if (this->bpm_ != nullptr) {
    Drop();
  }
  // 继承别人的
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  // 把别人清空

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

// 使用完这个页面就要告诉缓冲池
void BasicPageGuard::Drop() {
  // 不为空
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), is_dirty_);
    // 把这个页面的pin改成0，这样就能够使用RLU回收页面了,如果修改了就要穿进去，所以是在这里设置dirty这个参数
    is_dirty_ = false;
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    // 继承别人的
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    // 把别人清空

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

// Read里面有一个basicPageGuard，Read和他们不是继承关系，他也有自己的Drop
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  Drop();
  guard_ = BasicPageGuard(std::move(that.guard_));
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();  // 释放锁，这是一个共享读锁，和写锁互斥，但是与其他共享读锁不互斥，那什么时候上锁呢？？？
  guard_.Drop();
  // 就是guard里面有一个basicx对象，对象里面管理着一个缓冲池，和一个页面号，以及一个是否脏读，使用read
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  Drop();
  guard_ = BasicPageGuard(std::move(that.guard_));
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();  // 释放锁，这是一个共享读锁，和写锁互斥，但是与其他共享读锁不互斥，那什么时候上锁呢？？？
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
