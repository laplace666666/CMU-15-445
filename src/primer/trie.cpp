#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  // 空树
  if (!root_) {
    return nullptr;
  }
  auto ptr = root_;
  for (auto c : key) {
    if (ptr->children_.count(c) != 0) {
      // 存在
      ptr = ptr->children_.at(c);
    } else {
      return nullptr;
    }
  }
  // 到达终止节点
  if (!ptr->is_value_node_) {
    return nullptr;
  }
  // 即使是终止节点，类型匹配吗， 就是数值的类型
  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());
  if (value != nullptr) {
    return value->value_.get();
  }
  return nullptr;
  //   You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  //   nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  //   dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  //   Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  if (!root_) {
    auto trienode = std::make_shared<TrieNode>();
    // 变成trie之后才能调用Put方法
    return Trie(trienode).Put(key, std::move(value));
  }
  // 如果不是空树，就沿着跟一直往下找，但是要先创建一个新的根
  std::shared_ptr<TrieNode> root = root_->Clone();  // 克隆下来
  auto ptr = root;                                  // 保留根节点方便返回
  if (key.empty()) {
    // 这时候需要要根节点改成数值型
    ptr = std::make_shared<TrieNodeWithValue<T>>(ptr->children_, std::make_shared<T>(std::move(value)));
    return Trie(ptr);
  }
  // 这时候就处理完了两个边界情况,
  size_t i = 0;
  // 因为最后一个节点可能要修改成value类型
  for (; i < key.size() - 1; i++) {
    if (ptr->children_.count(key[i]) == 0) {
      // 不存在,新创建一个节点
      std::shared_ptr<TrieNode> node = std::make_shared<TrieNode>();
      ptr->children_[key[i]] = node;
      ptr = node;
    } else {
      // 存在的话，要克隆一下，要创建一个新的节点
      std::shared_ptr<TrieNode> new_node = ptr->children_.at(key[i])->Clone();
      ptr->children_[key[i]] = new_node;
      ptr = new_node;
    }
  }
  // 现在就要看最后一个节点，是添加还是修改
  std::shared_ptr<TrieNodeWithValue<T>> value_node;
  if (ptr->children_.count(key.back()) == 0) {
    // 不存在就创建一个新的
    value_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    // 存在就覆盖，还需要克隆其子类的关系
    value_node = std::make_shared<TrieNodeWithValue<T>>(ptr->children_[key.back()]->children_,
                                                        std::make_shared<T>(std::move(value)));
  }
  ptr->children_[key.back()] = value_node;
  return Trie(root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

std::shared_ptr<const TrieNode> Dfs(const std::shared_ptr<const TrieNode> &root, std::string_view key, size_t index) {
  // 深度优先搜索的出口，也就说找到了这个key的最后一个分支，我们来分析一下为什么要index等于key.size()不是key.size()-1
  // 等于key.size()就相当于已经找到了最后一个指向数值的分支
  if (index == key.size()) {
    if (root->children_.empty()) {
      return nullptr;
      // 就是找到最后没有孩子了，叶子节点，要删除他就a返回一个空指针就可以了
    }
    return std::make_shared<const TrieNode>(root->children_);
    // 如果不是叶子节点就复制其分支，返回就相当于删除了他的值
  }
  std::shared_ptr<const TrieNode> new_node;          // 创建一个指向新节点的指针
  auto t = std::const_pointer_cast<TrieNode>(root);  // 把root转换成非const类型
  if (t->children_.find(key[index]) != root->children_.end()) {
    new_node = Dfs(t->children_[key[index]], key, index + 1);
    // 继续往下找，并返回root的clone或者本身，返回值如果是nullptr
    auto node = root->Clone();  // 为什么克隆，因为这个roote可能还有其他的链接
    if (new_node) {
      node->children_[key[index]] = new_node;  // 如果不是空值，就把返回的这个非值的trienodea加入到这里面来
    } else {
      // 如果是空值就这个键值对，删除之后，如果roote既没有值，又没有连接了，就需要把这个节点也删除了，继续往上传递
      node->children_.erase(key[index]);
      if (!node->is_value_node_ && node->children_.empty()) {
        return nullptr;
      }
    }
    return node;  // 前面没有返回控制，现在就返回这个clone的node，自此往上的过程都是需要克隆的
  }
  return root;  // 如果没有找到，删除就没有意义了，就直接返回，还返回他自己原路返回，找不到就返回了。
  // 思路就是往下找，找到了就克隆一个没有值的trienode，返回，找不到就返回原来的节点，如果最后一个点事根节点就返回nullptr并删除这个键值对，如果删除之后没有连接逐级向上删除
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto root = Dfs(root_, key, 0);  // 深度优先搜索
  return Trie(root);
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
