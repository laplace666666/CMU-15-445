#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

// 上下文的释放
Context::~Context() {
  write_set_.clear();
  read_set_.clear();
  header_page_.reset();
  root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  // 申请了一个读页面，来用来存储head_page
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();  // 这说明b+树在初始化的时候就开辟了一个页来存储head_page
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {  // 以读的方式获取到根页面
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  // 强制转换为BPlusTreeHeaderPage
  auto *header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;  // 这个是上下文，用于记录访问的页面
  (void)ctx;    // 对未使用变量抑制警告
                // 先获取叶子节点的id
  page_id_t leaf_page_id = GetKeyAt(key, comparator_, ctx);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 这里只要有根节点，就能够确定他在哪个叶子节点里面，叶子节点没有再返回false
  // 先获取叶子节点的守卫
  auto leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto *leaf_page = leaf_page_guard.As<LeafPage>();
  // 在叶子节点中查找
  int i = leaf_page->Lookup(key, comparator_);
  bool is_success = false;
  if (i >= 0 && i < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(i), key) == 0) {
    // 这个代码有疑问不知道写不写
    BUSTUB_ASSERT(result != nullptr, "result not nullptr");
    result->push_back(leaf_page->ValueAt(i));
    is_success = true;
  }
  return is_success;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  // 这个函数的功能就是找到key对应的叶子页号
  // 先找到head_page读取其中的root_page_id
  // 读取存储head_page的页
  ReadPageGuard header_page_gurad = bpm_->FetchPageRead(header_page_id_);
  // 现在就转换成了这个head指针指向对应的页数据，原来char类型指针指向数据，每次只能读取一个字符的数据，不是这个指针读不出来这个数据
  auto *p_header_page = header_page_gurad.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = p_header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  // 找到了root_id，用上下文类来记录读取信息
  ctx.root_page_id_ = root_page_id;
  ctx.read_set_.push_back(std::move(header_page_gurad));

  // 开始读取根页面
  ReadPageGuard root_page_guard = bpm_->FetchPageRead(root_page_id);
  auto *root_page = root_page_guard.As<InternalPage>();
  // 转化成对应的页面指针，每次读取的数据大小来能使用
  ctx.access_set_.push_back(root_page_id);
  ctx.read_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    // 如果不是叶子页面
    // 在内部页面上二分查找
    int i = root_page->Lookup(key, comparator);  // 其实找到的是第一个大于等于的位置
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      // 这个就是要叶子页的page_id
      root_page_id = root_page->ValueAt(i);
    } else {
      // 其他情况的话，因为如果等于就是id，大于就是下一个id
      root_page_id = root_page->ValueAt(i - 1);
      // 不理解就去读Lookup
    }
    // 读取下一个页面
    root_page_guard = bpm_->FetchPageRead(root_page_id);
    root_page = root_page_guard.As<InternalPage>();
    ctx.read_set_.pop_back();
    ctx.read_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);  // 加锁了
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    bpm_->NewPageGuarded(&root_page_id);
    auto write_guard = bpm_->FetchPageWrite(root_page_id);
    auto *p_leaf_page = write_guard.AsMut<BPlusTree::LeafPage>();
    p_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
    p_leaf_page->SetMaxSize(leaf_max_size_);
    p_leaf_page->SetNextPageId(INVALID_PAGE_ID);
    p_leaf_page->SetSize(0);
    SetRootPageId(root_page_id, ctx);
    ctx.write_set_.push_back(std::move(write_guard));
    ctx.access_set_.push_back(root_page_id);
    ctx.root_page_id_ = root_page_id;
    return root_page_id;
  }
  ctx.root_page_id_ = root_page_id;
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
  ctx.access_set_.push_back(root_page_id);
  ctx.write_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    int i = root_page->Lookup(key, comparator);
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->ValueAt(i);
    } else {
      root_page_id = root_page->ValueAt(i - 1);
    }
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
    // crabbing lock, unlock all parent page when current lock is safe
    // safe status : insert one node without split on this node.
    if (root_page->GetSize() + 1 < root_page->GetMaxSize()) {
      if (ctx.header_page_ != std::nullopt) {
        ctx.header_page_.reset();
      }
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id, Context &ctx) {
  auto guard = std::move(ctx.header_page_);
  auto *header_page = guard->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
  ctx.root_page_id_ = page_id;
  ctx.header_page_ = std::move(guard);
  // guard里面有一个页面指针，指针可以直接访问内存，这个转化也是转化成
  // BPlusTreeHeaderPage指针
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;  // Suppresses unused variable warning.
  bool is_success;
  // Get the leaf page ID where the new (key, value) pair should be inserted.
  page_id_t leaf_page_id = InsertGetKeyAt(key, comparator_, ctx);

  // Get the lock for the leaf page.
  WritePageGuard leaf_page_guard = std::move(ctx.write_set_.back());  // get leaf page lock
  ctx.write_set_.pop_back();                                          //
  auto *leaf_page = leaf_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  // Find the index of the key in the leaf page.
  int index = leaf_page->Lookup(key, comparator_);

  // If the key already exists in the tree, return false.
  if (index > 0 && index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    // 已经存在
    is_success = false;
  } else {
    // If there is enough space in the leaf page to insert the new (key, value) pair, insert it.
    if (leaf_page->GetSize() + 1 < leaf_page->GetMaxSize()) {
      leaf_page->Insert(key, value, comparator_);
    } else {
      // If there is not enough space in the leaf page, create a new leaf page and redistribute the keys.
      page_id_t leaf_page_id_new;
      bpm_->NewPageGuarded(&leaf_page_id_new);
      auto leaf_page_new_guard = bpm_->FetchPageWrite(leaf_page_id_new);
      auto *leaf_page_new = leaf_page_new_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      leaf_page_new->SetMaxSize(leaf_max_size_);
      leaf_page_new->SetSize(0);
      leaf_page_new->SetPageType(IndexPageType::LEAF_PAGE);
      leaf_page_new->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->MoveHalfTo(leaf_page_new);
      // Determine whether to insert the new (key, value) pair in the old leaf page or the new leaf page.
      leaf_page->SetNextPageId(leaf_page_id_new);
      if (index <= (leaf_page->GetMaxSize() - 1) / 2) {
        leaf_page->Insert(key, value, comparator_);
      } else {
        leaf_page_new->MoveFirstToEndOf(leaf_page);
        leaf_page_new->Insert(key, value, comparator_);
      }
      // Get the key at the first position of the new leaf page and insert it into the parent node.
      KeyType mid_key = leaf_page_new->KeyAt(0);
      InsertInParent(leaf_page_id, mid_key, leaf_page_id_new, ctx);
    }
    is_success = true;
  }
  return is_success;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(page_id_t leaf_page_left_id, KeyType key, page_id_t leaf_page_right_id,
                                    Context &ctx) {
  // Get the id of the root page.
  page_id_t root_page_id = ctx.root_page_id_;

  // If the root page does not exist, create a new root page with the key-value pair as its first entry.
  if (root_page_id == leaf_page_left_id) {
    // leafPage is root page
    page_id_t root_page_new_id;
    bpm_->NewPageGuarded(&root_page_new_id);

    // Find the leaf page where the key-value pair should be inserted.
    auto root_page_new_guard = bpm_->FetchPageWrite(root_page_new_id);
    //    auto root_page_new_guard = ctx.GetWritePageGuardAt(bpm_,root_page_new_id);
    auto *root_page_new =
        root_page_new_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    root_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
    root_page_new->SetMaxSize(internal_max_size_);
    root_page_new->SetSize(0);
    root_page_new->InsertFirstOf(leaf_page_left_id);
    root_page_new->Insert(key, leaf_page_right_id, comparator_);
    SetRootPageId(root_page_new_id, ctx);
  } else {
    page_id_t parent_page_id = GetParentPageId(leaf_page_left_id, ctx);
    //    auto parent_page_guard = bpm_->FetchPageWrite(parent_page_id);
    auto parent_page_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto *parent_page = parent_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (parent_page->GetSize() < parent_page->GetMaxSize()) {
      // 够就直接插入
      parent_page->Insert(key, leaf_page_right_id, comparator_);
    } else {
      // 内部节点split
      int index = parent_page->Lookup(key, comparator_);
      page_id_t parent_page_new_id;
      bpm_->NewPageGuarded(&parent_page_new_id);
      auto parent_page_new_guard = bpm_->FetchPageWrite(parent_page_new_id);
      //      auto parent_page_new_guard = ctx.GetWritePageGuardAt(bpm_,parent_page_new_id);
      auto *parent_page_new = parent_page_new_guard.template AsMut<BPlusTree::InternalPage>();
      parent_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
      parent_page_new->SetMaxSize(internal_max_size_);
      parent_page_new->SetSize(0);
      parent_page->MoveHalfTo(parent_page_new);
      // index >= ceil((n+1)/2)
      if (index >= ((parent_page->GetMaxSize() + 1) + 1) / 2) {
        parent_page_new->MoveFirstToEndOf(parent_page);
        parent_page_new->Insert(key, leaf_page_right_id, comparator_);
      } else {
        parent_page->Insert(key, leaf_page_right_id, comparator_);
      }
      KeyType mid_key = parent_page_new->KeyAt(1);
      page_id_t mid_page_id = parent_page_new->ValueAt(1);
      parent_page_new->EraseAt(1);
      parent_page_new->EraseAt(0);
      parent_page_new->InsertFirstOf(mid_page_id);
      InsertInParent(parent_page_id, mid_key, parent_page_new_id, ctx);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetParentPageId(page_id_t child, Context &ctx) -> page_id_t {
  page_id_t parent_id = INVALID_PAGE_ID;
  for (auto it = ctx.access_set_.begin(); (*it) != child; it++) {
    parent_id = (*it);
  }
  return parent_id;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  page_id_t leaf_page_id = DeleteGetKeyAt(key, comparator_, ctx);
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // 下面就指向删除操作
  RemoveEntry(leaf_page_id, key, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;
  // 没有根页面就直接返回一个错误页面
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
  ctx.access_set_.push_back(root_page_id);
  ctx.write_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    int i = root_page->Lookup(key, comparator);
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->ValueAt(i);
    } else {
      root_page_id = root_page->ValueAt(i - 1);
    }
    // 这个是加锁，读的时候就加锁了
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
    // 这一部分的作用就是他们的孩子合并之后自己也不会合并，所以就没必要记录了，
    if (root_page->GetSize() - 1 >= root_page->GetMinSize()) {
      ctx.header_page_.reset();
      ctx.write_set_.clear();
    }
    // Page_guard移动语义之后就解锁了，调用Drop函数，其他的数据都移动过去了，锁释放了
    ctx.write_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(page_id_t basic_page_id, const KeyType &key, Context &ctx) {
  // 先获取根页面
  WritePageGuard basic_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *basic_page = basic_page_guard.AsMut<BPlusTreePage>();
  bool is_success = true;
  // 先删除样本
  if (basic_page->IsLeafPage()) {
    auto *leaf_page = basic_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    is_success |= leaf_page->RemoveKeyAt(key, comparator_);
  } else {
    auto *internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    is_success |= internal_page->RemoveKeyAt(key, comparator_);
  }
  // 下面根据删除后的状况做出调整，返回false就是没有做出调整还是正确的
  if (!is_success) {
    return;
  }
  int root_page_id = ctx.root_page_id_;
  if (basic_page_id == root_page_id && basic_page->GetSize() == 0) {
    SetTreeEmpty(ctx);               // 根页面删空了
    bpm_->DeletePage(root_page_id);  // 就把这片存储空间删除
  } else if (basic_page_id == root_page_id && basic_page->GetSize() == 1 && !basic_page->IsLeafPage()) {
    // 删除的页面是跟页面且删除之后就剩一个元素，并且不是叶子页面是内部页面
    auto *root_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    SetRootPageId(root_page->ValueAt(0), ctx);
    bpm_->DeletePage(root_page_id);
  } else if (basic_page_id != root_page_id && basic_page->GetSize() < basic_page->GetMinSize()) {
    // 这是删除之后出现半满的情况，需要合并或者重分配操作
    page_id_t parent_page_id = GetParentPageId(basic_page_id, ctx);
    BPlusTree::InternalPage *parent_page;
    WritePageGuard parent_page_guard;
    if (parent_page_id != INVALID_PAGE_ID) {
      parent_page_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      parent_page = parent_page_guard.AsMut<BPlusTree::InternalPage>();
    }
    // 读取到他的父页面
    auto pair = GetSiblingPageId(parent_page, key, ctx);
    KeyType mid_key = pair.second;      // 就是可能要删除的key
    page_id_t sibling_id = pair.first;  // 他前面或者后面的兄弟页面
    WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_id);
    auto *sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
    // 下面就要尝试合并或者重分配操作，但是这个时候需要区分当前节点是叶子页面还是内部页面
    // 合并
    if (sibling_page->GetSize() - 1 < sibling_page->GetMinSize()) {
      // 就说明两者可以合并，减1是因为basic删除了一个1没有达到半满
      int index = parent_page->Lookup(key, comparator_);
      if (index == 1 && comparator_(key, parent_page->KeyAt(1)) < 0) {
        // 只有这个时候sibling在basic后面
        std::swap(basic_page, sibling_page);  // 交换指针指向
        WritePageGuard tmp = std::move(basic_page_guard);
        basic_page_guard = std::move(sibling_page_guard);
        sibling_page_guard = std::move(tmp);
        std::swap(basic_page_id, sibling_id);
        // 相当于什么都换了
      }
      // 下面区分是叶子节点还是内部节点
      if (!basic_page->IsLeafPage()) {
        // 内部节点的合并比较复杂
        auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
        auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
        page_id_t mid_key_page_id = basic_internal_page->ValueAt(0);
        // 先把父节点的那个降下来
        sibling_internal_page->Insert(mid_key, mid_key_page_id, comparator_);
        // 再把全部的移动过去,注意是从1开始移动的，因为0号数据没有删除
        basic_internal_page->MoveAllTo(sibling_internal_page);
      } else {
        // 叶子页面
        // 与leafpage sibling节点合并，直接move即可
        auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
        auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
        basic_leaf_page->MoveAllTo(sibling_leaf_page);
        // 叶子节点的前后是需要建立连接的
        sibling_leaf_page->SetNextPageId(basic_leaf_page->GetNextPageId());
      }
      // 下面就是删除空出来的basic页面
      ctx.write_set_.push_back(std::move(parent_page_guard));  // 对父页面进行解锁
      RemoveEntry(parent_page_id, mid_key, ctx);               // 删除父页面对basic页面的指向
      bpm_->DeletePage(basic_page_id);                         // 删除空闲页面，空出空间
    } else {
      // 合并不了就要重分配，因为删除一个没有半满，那么就从兄弟页面拿一个过来
      int index = parent_page->Lookup(key, comparator_);
      if (index == 1 && comparator_(key, parent_page->KeyAt(1)) < 0) {
        // basic_page is the previous of sibling_page
        if (!basic_page->IsLeafPage()) {
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = 0;
          // 兄弟页面的第一个value（没有key）
          page_id_t first_page_id = sibling_internal_page->ValueAt(m);
          // 兄弟页面的第一个key
          KeyType first_key = sibling_internal_page->KeyAt(m + 1);
          // 把上面拉下来插入左边
          basic_internal_page->Insert(mid_key, first_page_id, comparator_);
          sibling_internal_page->EraseAt(0);
          sibling_internal_page->SetKeyAt(0, KeyType());
          // 将兄弟的最后一个key作为basic的第一个key，即array_[1].Key
          // 将兄弟的最后一个指针作为basic的第0个page_id，即array_[0].Value
          // 将自己的第一个指针作为basic的第一个page_id，即arraty_[1].Value
          // 父页面的key要换掉
          ReplaceKeyAt(parent_page, mid_key, first_key, ctx);
        } else {
          // 叶子节点的话，就把sibling的第一个移动到basic上去就行
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          sibling_leaf_page->MoveFirstToEndOf(basic_leaf_page);
          KeyType second_key = sibling_leaf_page->KeyAt(0);
          ReplaceKeyAt(parent_page, mid_key, second_key, ctx);
        }
      } else {
        // 这时候sibling就在basic前面，sibling最后移动一个到basic前面
        if (!basic_page->IsLeafPage()) {
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = sibling_internal_page->GetSize() - 1;
          // value移动到右边
          page_id_t last_page_id = sibling_internal_page->ValueAt(m);
          // key要上去
          KeyType last_key = sibling_internal_page->KeyAt(m);
          // sibling删除m
          sibling_internal_page->EraseAt(m);
          page_id_t basic_pointer_page_id = basic_internal_page->ValueAt(0);
          // 把左边移动过来，覆盖原来的0号数据
          basic_internal_page->SetValueAt(0, last_page_id);
          // 以插入的方式插入原来的0号数据
          basic_internal_page->Insert(mid_key, basic_pointer_page_id, comparator_);
          ReplaceKeyAt(parent_page, mid_key, last_key, ctx);
        } else {
          // 叶子节点就直接把他左边最后移动过去
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          int m = sibling_leaf_page->GetSize() - 1;
          ValueType last_value = sibling_leaf_page->ValueAt(m);
          KeyType last_key = sibling_leaf_page->KeyAt(m);
          sibling_leaf_page->RemoveAt(m);
          basic_leaf_page->Insert(last_key, last_value, comparator_);
          ReplaceKeyAt(parent_page, mid_key, last_key, ctx);
        }
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetTreeEmpty(Context &ctx) {
  auto header_page = std::move(ctx.header_page_);
  auto *p_header_page = header_page->AsMut<BPlusTreeHeaderPage>();
  p_header_page->root_page_id_ = INVALID_PAGE_ID;
  ctx.header_page_ = std::move(header_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSiblingPageId(const BPlusTree::InternalPage *parent_page, const KeyType &key, Context &ctx) {
  page_id_t sibling_page_id;
  KeyType parent_key;                                 // basic_page对应的parent的那个key
  int index = parent_page->Lookup(key, comparator_);  // 找到第一个大于等于key的index，array_[index].first的
  int n = parent_page->GetSize();
  if (index == n) {
    // 就是最后一个，其实兄弟就是他前面的那一个 sibling在basic前面
    sibling_page_id = parent_page->ValueAt(index - 2);
    parent_key = parent_page->KeyAt(index - 1);
  } else if (index > 1 && index <= n - 1) {
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      // 找到的这个就是对应的当前节点 sibling在basic前面
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      // sibling在basic前面
      sibling_page_id = parent_page->ValueAt(index - 2);
      parent_key = parent_page->KeyAt(index - 1);
    }
  } else {
    // index=1
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      // 找到的这个就是对应的当前节点 sibling在basic前面
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      // 这时候这个sibling在basic后面
      sibling_page_id = parent_page->ValueAt(index);
      parent_key = parent_page->KeyAt(index);
      // 这块我不是很理解，没有key怎么办，因为这个就是第一个key
    }
  }
  return std::make_pair(sibling_page_id, parent_key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReplaceKeyAt(BPlusTree::InternalPage *page, const KeyType &src, const KeyType &dst, Context &ctx) {
  int index = page->Lookup(src, comparator_);
  BUSTUB_ASSERT(!(index < 0 || index >= page->GetSize()), "ReplaceKeyAt page_id source_key not in page");
  page->SetKeyAt(index, dst);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // Context ctx;
  auto root_page_id = GetRootPageId();
  //  ReadPageGuard root_page_guard = ctx.GetReadPageGuardAt(bpm_,root_page_id);
  if (root_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  BasicPageGuard root_page_guard = bpm_->FetchPageBasic(root_page_id);
  auto *root_page = root_page_guard.As<BPlusTree::InternalPage>();
  while (!root_page->IsLeafPage()) {
    root_page_id = root_page->ValueAt(0);
    if (root_page_id != INVALID_PAGE_ID) {
      root_page_guard = bpm_->FetchPageBasic(root_page_id);
      root_page = root_page_guard.As<BPlusTree::InternalPage>();
    }
  }
  auto *leaf_page = root_page_guard.As<BPlusTree::LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, leaf_page, 0, std::move(root_page_guard));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  (void)ctx;
  // 找到所在的页面
  auto page_id = GetKeyAt(key, comparator_, ctx);
  ctx.read_set_.clear();
  if (page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  BasicPageGuard leaf_page_guard = bpm_->FetchPageBasic(page_id);
  ctx.read_set_.pop_back();
  const auto *leaf_page = leaf_page_guard.As<BPlusTree::LeafPage>();
  int index = leaf_page->Lookup(key, comparator_);
  if (comparator_(leaf_page->KeyAt(index), key) != 0) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(bpm_, leaf_page, index, std::move(leaf_page_guard));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, nullptr, -1, BasicPageGuard()); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *p_header_page = page_guard.As<BPlusTreeHeaderPage>();
  return p_header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
