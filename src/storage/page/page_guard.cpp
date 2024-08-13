#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
// 总共有三个属性，一个是缓冲池，缓冲池中的那个页面指针，这个页面是否脏了,这个是移动构造
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

// 使用完这个页面就要告诉缓冲池
void BasicPageGuard::Drop() {
  // 不为空
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), is_dirty_);
    // 把这个页面的pin改成0，这样就能够使用RLU回收页面了,如果修改了就要穿进去，所以是在这里设置dirty这个参数
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  that.page_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT 自动析构释放锁了

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
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    //    printf("Drop pageGuard page_id=%d\n", guard_.page_->GetPageId());
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
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
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    //    printf("Drop pageGuard page_id=%d\n", guard_.page_->GetPageId());
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
