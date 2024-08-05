#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code: 注意看返回值，返回值是一个可选项
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  // 伪代码：
  // （1）获取根锁，获取根节点，然后释放根锁。在持有根锁期间不要在 trie 中查找值。
  // （2）在 trie 中查找值。
  // （3）如果找到值，返回一个 ValueGuard 对象，该对象持有对值和根的引用。否则，返回 std::nullopt 。
  // throw NotImplementedException("TrieStore::Get is not implemented.");
  root_lock_.lock();
  auto root = root_;
  root_lock_.unlock();
  const T *value = root.Get<T>(key);
  // 如果找到了值（即 value 不为空指针），则创建并返回一个 ValueGuard<T> 对象，该对象包含了找到的值和 root 。
  if (value != nullptr) {
    return ValueGuard<T>(root, *value);  // 其实调用这个*解引用的时候就已经返回值了
  }
  return std::nullopt;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Put is not implemented.");
  write_lock_.lock();  // 只要别人不释放，就不能写
  root_lock_.lock();   // 这个早点释放，其他进程还可以读取
  auto root = this->root_;
  root_lock_.unlock();
  root = root.Put(key, std::move(value));
  root_lock_.lock();
  root_ = root;
  root_lock_.unlock();
  write_lock_.unlock();
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  // throw NotImplementedException("TrieStore::Remove is not implemented.");
  write_lock_.lock();
  root_lock_.lock();
  auto root = this->root_;
  root_lock_.unlock();
  root = root.Remove(key);
  root_lock_.lock();
  root_ = root;
  root_lock_.unlock();
  write_lock_.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
