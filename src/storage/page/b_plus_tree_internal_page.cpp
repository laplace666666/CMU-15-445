//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;  // 按照前面介绍的是存储是key和page_id，i
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> int {
  // 内部节点第一个key是空，因为是key存的page_id是大于等于key的数据位置
  // 所以这次查找可能找不到相等的key，只需要找到大于等于index的最小的key
  // 基于这个算法，最终的即使找不到对应的，他也会找到大于等于index的最小的key
  int l = 1;
  int r = GetSize() - 1;
  int ans = r + 1;  // 先把他放到最后一个
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first, key) >= 0) {
      ans = mid;
      r = mid - 1;  // ans记录的是r的右边一位，说明ans是比key大的，当key足够大的时候就不会进入这部分，所以预设等于r+1
    } else {
      l = mid + 1;
    }
  }
  return ans;  // 总的来说返回的是大于等于key的第一个索引位置
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirstOf(const page_id_t &value) {
  int n = GetSize();
  for (int i = n; i > 0; i--) {
    std::swap(array_[i], array_[i - 1]);
  }
  array_[0] = std::make_pair(KeyType(), value);
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const page_id_t &value, const KeyComparator &comparator)
    -> int {
  int index = Lookup(key, comparator);
  MappingType tmp = array_[index];
  int n = GetSize();
  array_[index] = std::make_pair(key, value);
  for (int i = index + 1; i <= n; i++) {
    std::swap(tmp, array_[i]);
  }
  IncreaseSize(1);
  return GetSize();
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  int n = GetSize();
  int j = 1;
  for (int i = n / 2; i < n; i++) {
    recipient->array_[j++] = array_[i];
  }
  recipient->IncreaseSize(j);
  this->IncreaseSize(-(j - 1));
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  // 把右边的第一个移动到左边的末尾
  int n = GetSize();
  MappingType tmp = array_[1];
  for (int i = 1; i < n; i++) {
    array_[i] = array_[i + 1];
  }
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(rn + 1 < recipient->GetMaxSize(),
                "B_PLUS_TREE_INTERNAL_PAGE_TYPE MoveFirstToEndOf recipient size + 1 < maxSize");
  recipient->array_[rn] = tmp;
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAt(int index) {
  int n = GetSize();
  for (int i = index; i < n - 1; i++) {
    std::swap(array_[i], array_[i + 1]);
  }
  this->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveEndToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  int n = recipient->GetSize();
  BUSTUB_ASSERT(n + 1 < recipient->GetMaxSize(), "MoveEndToFrontOf recipient full");
  MappingType tmp = recipient->array_[0];
  for (int i = 1; i <= n; i++) {
    std::swap(tmp, recipient->array_[i]);
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKeyAt(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = Lookup(key, comparator);
  int n = GetSize();
  bool is_success = false;
  if (index >= 0 && index < n) {
    EraseAt(index);
    is_success = true;
  }
  return is_success;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  int n = GetSize();
  int rn = recipient->GetSize();
  BUSTUB_ASSERT(n + rn - 2 < GetMaxSize(), "MoveAllto throw Exception beacause n+rn-1>=InternalMaxSize");
  for (int i = 1; i < n; i++) {
    recipient->array_[rn++] = array_[i];
  }
  recipient->IncreaseSize(n - 1);
  this->IncreaseSize(-(n - 1));
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
