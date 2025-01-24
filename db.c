#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_KEY_NOT_FOUND,
  EXECUTE_FAIL
} ExecuteResult;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT , STATEMENT_DELETE} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
  int delete_id;
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 400
#define MAX_NUM_LOADED_PAGES 10

#define INVALID_PAGE_NUM UINT32_MAX

typedef struct PinnedPageNode {
    uint32_t page_num;                // The page number
    struct PinnedPageNode* next;      // Pointer to the next node in the list
} PinnedPageNode;

typedef struct {
    PinnedPageNode* head;   // Points to the first node
    PinnedPageNode* tail;   // Points to the last node
} PinnedPages;

typedef struct LRUNode {
  uint32_t page_num;
  struct LRUNode* prev;
  struct LRUNode* next;
} LRUNode;

typedef struct {
  LRUNode* head;
  LRUNode* tail;
} LRU_List;

#define FREED_PAGES_STACK_SIZE (TABLE_MAX_PAGES * sizeof(uint32_t))
#define FREED_PAGES_START_OFFSET (FREED_PAGES_STACK_SIZE + sizeof(uint32_t))

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  char* pages[MAX_NUM_LOADED_PAGES];
  uint32_t freed_pages_stack[TABLE_MAX_PAGES];
  uint32_t freed_pages_count;
  LRU_List lru_list;
  uint32_t num_loaded_pages;
  int32_t page_numbers[TABLE_MAX_PAGES];
  bool pinned[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  Pager* pager;
  uint32_t root_page_num;
} Table;

typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE + 
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
/* Keep this small for testing */
const uint32_t INTERNAL_NODE_MAX_KEYS = 3;

/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

NodeType get_node_type(char* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(char* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(char* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(char* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(char* node) { return (uint32_t*)(node + PARENT_POINTER_OFFSET); }

uint32_t* internal_node_num_keys(char* node) {
  return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* internal_node_right_child(char* node) {
  return (uint32_t*)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* internal_node_cell(char* node, uint32_t cell_num) {
  return (uint32_t*)(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_child(char* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    uint32_t* right_child = internal_node_right_child(node);
    if (*right_child == INVALID_PAGE_NUM) {
      printf("Tried to access right child of node, but was invalid page\n");
      exit(EXIT_FAILURE);
    }
    return right_child;
  } else {
    uint32_t* child = internal_node_cell(node, child_num);

    if (*child == INVALID_PAGE_NUM) {
      printf("Tried to access child %d of node, but was invalid page\n", child_num);
      exit(EXIT_FAILURE);
    }
    return child;
  }
}

uint32_t* internal_node_key(char* node, uint32_t key_num) {
  return (uint32_t*)((char*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

uint32_t* leaf_node_num_cells(char* node) {
  return (uint32_t*)(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t* leaf_node_next_leaf(char* node) {
  return (uint32_t*)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

char* leaf_node_cell(char* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(char* node, uint32_t cell_num) {
  return (uint32_t*)(leaf_node_cell(node, cell_num));
}

char* leaf_node_value(char* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[pager->page_numbers[page_num]] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, FREED_PAGES_START_OFFSET + (page_num * PAGE_SIZE), SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[pager->page_numbers[page_num]], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void pin_page(Pager* pager, uint32_t page_num) {
  if (page_num < TABLE_MAX_PAGES) {
    pager->pinned[page_num] = true;
  } else {
    printf("Error: Attempted to pin an invalid page number %u\n", page_num);
  }
}

void unpin_page(Pager* pager, uint32_t page_num) {
  if (page_num < TABLE_MAX_PAGES) {
    pager->pinned[page_num] = false;
  } else {
    printf("Error: Attempted to unpin an invalid page number %u\n", page_num);
  }
}

PinnedPages* init_pinned_pages() {
    PinnedPages* tracker = (PinnedPages*)malloc(sizeof(PinnedPages));  // Dynamically allocate memory
    if (tracker == NULL) {
        // Handle allocation failure (e.g., return NULL or log error)
        printf("Error: Memory allocation for pinned pages tracker failed!\n");
        return NULL;
    }
    tracker->head = NULL;  // Initialize head to NULL
    tracker->tail = NULL;  // Initialize tail to NULL
    return tracker;  // Return the pointer to the allocated and initialized structure
}

void append_pinned_page(PinnedPages* tracker, uint32_t page_num) {
    PinnedPageNode* new_node = (PinnedPageNode*)malloc(sizeof(PinnedPageNode));
    new_node->page_num = page_num;
    new_node->next = NULL;

    if (tracker->tail == NULL) {
        // If the list is empty, the new node becomes both the head and tail
        tracker->head = new_node;
        tracker->tail = new_node;
    } else {
        // Append the new node to the end of the list
        tracker->tail->next = new_node;
        tracker->tail = new_node;
    }
}

void unpin_all_pages(Pager* pager, PinnedPages* tracker) {
    PinnedPageNode* current = tracker->head;

    // Unpin all pages
    while (current != NULL) {
        // Unpin the page using the unpin_page function
        unpin_page(pager, current->page_num);

        // Move to the next node in the linked list
        PinnedPageNode* temp = current;
        current = current->next;

        // Free the current node to release memory
        free(temp);
    }

    // After unpinning all pages, reset the linked list pointers
    tracker->head = NULL;
    tracker->tail = NULL;

    // Free the tracker structure itself
    free(tracker);

    // Set the tracker pointer to NULL to avoid dangling pointers
    tracker = NULL;
}

void lru_list_initialize(Pager* pager) {
  pager->lru_list.head = NULL;
  pager->lru_list.tail = NULL;
}

void remove_node(Pager* pager, LRUNode* node_to_remove) {
   if (!node_to_remove) return;

    // Update the previous node's next pointer
    if (node_to_remove->prev) {
        node_to_remove->prev->next = node_to_remove->next;
    }

    // Update the next node's previous pointer
    if (node_to_remove->next) {
        node_to_remove->next->prev = node_to_remove->prev;
    }

    // Update the head or tail pointers if necessary
    if (node_to_remove == pager->lru_list.head) {
        pager->lru_list.head = node_to_remove->next;
    }
    if (node_to_remove == pager->lru_list.tail) {
        pager->lru_list.tail = node_to_remove->prev;
    }

    free(node_to_remove);
    node_to_remove = NULL;
    pager->num_loaded_pages -= 1;
}

void add_page_to_lru(Pager* pager, uint32_t page_num) {
  LRUNode* current = pager->lru_list.head;
  while (current) {
    if (current->page_num == page_num) {
        remove_node(pager, current);
        break;
      }
    current = current->next;
  }

  LRUNode* new_node = malloc(sizeof(LRUNode));
    if (!new_node) {
        printf("Failed to allocate LRUNode\n");
        exit(EXIT_FAILURE);
    }
  new_node->page_num = page_num;
  new_node->prev = NULL;
  new_node->next = pager->lru_list.head;

  if (pager->lru_list.head) {
    pager->lru_list.head->prev = new_node;
  }
  pager->lru_list.head = new_node;

  if (!pager->lru_list.tail) {
    pager->lru_list.tail = new_node;
  }

  pager->num_loaded_pages += 1;
}

uint32_t remove_least_recently_used(Pager* pager) {
    if (!pager->lru_list.tail) {
        printf("Nothing to remove\n");
        exit(EXIT_FAILURE); // Nothing to remove
    }

    LRUNode* node_to_remove = pager->lru_list.tail;

    while (node_to_remove && pager->pinned[node_to_remove->page_num]) {
      node_to_remove = node_to_remove->prev;
    }

    if (!node_to_remove) {
      printf("No unpinned pages found in LRU list\n");
      exit(EXIT_FAILURE);
    }
    uint32_t remove_least_recently_used_page_num = node_to_remove->page_num;

    remove_node(pager, node_to_remove);
    return remove_least_recently_used_page_num;
}

char* get_page(Pager* pager, uint32_t page_num, PinnedPages* tracker) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }
  pin_page(pager, page_num);
  add_page_to_lru(pager, page_num);
  append_pinned_page(tracker, page_num);

  if (pager->page_numbers[page_num] == -1) {
    // Cache miss. Allocate memory and load from file.
    char* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, FREED_PAGES_START_OFFSET + (page_num * PAGE_SIZE), SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }

    if (pager->num_loaded_pages > MAX_NUM_LOADED_PAGES) {
      uint32_t page_to_evict = remove_least_recently_used(pager);
      lseek(pager->file_descriptor, FREED_PAGES_START_OFFSET + (page_to_evict * PAGE_SIZE), SEEK_SET);
      ssize_t bytes_written = write(pager->file_descriptor, pager->pages[pager->page_numbers[page_to_evict]], PAGE_SIZE);

      off_t total_file_length = lseek(pager->file_descriptor, 0, SEEK_END);
      pager->file_length = total_file_length - FREED_PAGES_START_OFFSET;

      free(pager->pages[pager->page_numbers[page_to_evict]]);
      pager->pages[pager->page_numbers[page_to_evict]] = NULL;
      pager->page_numbers[page_num] = pager->page_numbers[page_to_evict];
      pager->page_numbers[page_to_evict]  = -1;
    }
    else {
      pager->page_numbers[page_num] = pager->num_loaded_pages - 1;
    }
    pager->pages[pager->page_numbers[page_num]] = page;

  }
  return pager->pages[pager->page_numbers[page_num]];
}

uint32_t get_node_max_key(Pager* pager, char* node) {
  PinnedPages* tracker = init_pinned_pages();

  if (get_node_type(node) == NODE_LEAF) {
    uint32_t max_key = *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    unpin_all_pages(pager, tracker);
    return max_key;
  }
  char* right_child = get_page(pager,*internal_node_right_child(node), tracker);
  
  uint32_t max_key = get_node_max_key(pager, right_child);

  unpin_all_pages(pager, tracker);

  return max_key;
}

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}


void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  PinnedPages* tracker = init_pinned_pages();
  char* node = get_page(pager, page_num, tracker);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }

      unpin_all_pages(pager, tracker);
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
        for (uint32_t i = 0; i < num_keys; i++) {
          child = *internal_node_child(node, i);
          print_tree(pager, child, indentation_level + 1);

          indent(indentation_level + 1);
          printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
      }
      unpin_all_pages(pager, tracker);
      break;
  }
}

void serialize_row(Row* source, char* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(char* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void initialize_leaf_node(char* node) {
  memset(node, 0, PAGE_SIZE);
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}
void initialize_internal_node(char* node) {
  memset(node, 0, PAGE_SIZE);
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
  /*
  Necessary because the root page number is 0; by not initializing an internal 
  node's right child to an invalid page number when initializing the node, we may
  end up with 0 as the node's right child, which makes the node a parent of the root
  */
  *internal_node_right_child(node) = INVALID_PAGE_NUM;
}


Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  PinnedPages* tracker = init_pinned_pages();
  char* node = get_page(table->pager, page_num, tracker);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;

  // Binary search
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  unpin_all_pages(table->pager, tracker);
  return cursor;
}

uint32_t internal_node_find_child(char* node, uint32_t key) {
  /*
  Return the index of the child which should contain
  the given key.
  */

  uint32_t num_keys = *internal_node_num_keys(node);

  /* Binary search */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; /* there is one more child than key */
  while (min_index != max_index) { 
    uint32_t index = (min_index + max_index) / 2;
    uint32_t* key_ptr = internal_node_key(node, index);
    // Check if the pointer returned is NULL (invalid pointer)
    if (key_ptr == NULL) {
        printf("Error: internal_node_key returned NULL pointer for index %d\n", index);
        exit(EXIT_FAILURE);  // or handle it as appropriate
    }

    // Now it's safe to dereference key_ptr
    uint32_t key_to_right = *key_ptr;
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }
  return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  PinnedPages* tracker = init_pinned_pages();
  char* node = get_page(table->pager, page_num, tracker);

  uint32_t child_index = internal_node_find_child(node, key);

  uint32_t child_num = *internal_node_child(node, child_index);
  char* child = get_page(table->pager, child_num, tracker);

  Cursor* cursor;
  switch (get_node_type(child)) {
    case NODE_LEAF:
      cursor = leaf_node_find(table, child_num, key);
      break;
    case NODE_INTERNAL:
      cursor = internal_node_find(table, child_num, key);
      break;
    default:
      exit(EXIT_FAILURE);
  }

  unpin_all_pages(table->pager, tracker);
  return cursor;
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
  PinnedPages* tracker = init_pinned_pages();
 
  uint32_t root_page_num = table->root_page_num;
  char* root_node = get_page(table->pager, root_page_num, tracker);

  NodeType root_type = get_node_type(root_node);

  Cursor* cursor;

  if (get_node_type(root_node) == NODE_LEAF) {
    cursor = leaf_node_find(table, root_page_num, key);
  } else {
    cursor = internal_node_find(table, root_page_num, key);
  }

  unpin_all_pages(table->pager, tracker);
  return cursor;
}

Cursor* table_start(Table* table) {
  PinnedPages* tracker = init_pinned_pages();

  Cursor* cursor = table_find(table, 0);
  char* node = get_page(table->pager, cursor->page_num, tracker);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  unpin_all_pages(table->pager, tracker);
  return cursor;
}

char* cursor_value(Cursor* cursor) {
  PinnedPages* tracker = init_pinned_pages();

  uint32_t page_num = cursor->page_num;
  char* page = get_page(cursor->table->pager, page_num, tracker);

  unpin_all_pages(cursor->table->pager, tracker);
  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  PinnedPages* tracker = init_pinned_pages();

  uint32_t page_num = cursor->page_num;
  char* node = get_page(cursor->table->pager, page_num, tracker);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    /* Advance to next leaf node */
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      /* This was rightmost leaf */
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
  unpin_all_pages(cursor->table->pager, tracker);
}

bool is_empty_stack(Pager* pager) {
  return pager->freed_pages_count <= 0;
}

bool is_full_stack(Pager* pager) {
  return pager->freed_pages_count >= TABLE_MAX_PAGES;
}

void push_free_page(Pager* pager, uint32_t page_num) {
   if (is_full_stack(pager)) {
    printf("Stack overflow: cannot push page number %u.\n", page_num);
    return;
  }
  else {
    pager->freed_pages_stack[pager->freed_pages_count] = page_num;
    pager->freed_pages_count += 1;
  }
}

uint32_t pop_free_page(Pager* pager) {
  if (is_empty_stack(pager)) {
    return -1; // Indicate failure
  }
  else {
    pager->freed_pages_count -= 1;
    return pager->freed_pages_stack[pager->freed_pages_count];
  }
}

Pager* pager_open(const char* filename) {
  int fd = open(filename,
                O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // Uexeser write permission
                    S_IRUSR   // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  Pager* pager = malloc(sizeof(Pager));
  if (pager == NULL) {
  perror("Unable to allocate memory for Pager");
  exit(EXIT_FAILURE);
}
  memset(pager, 0, sizeof(Pager));

  pager->file_descriptor = fd;

  off_t file_size = lseek(fd, 0, SEEK_END);

  if (file_size == 0) {
    pager->freed_pages_count = 0;
    pager->file_length = 0;
    pager->num_pages = 0;
  }
  else {    
    off_t freed_stack_offset_position = lseek(fd, 0, SEEK_SET);
    if (freed_stack_offset_position == -1) {
      perror("Error seeking to freed pages stack");
      exit(EXIT_FAILURE);
    }

    // Read the count of freed pages
    ssize_t bytes_read = read(fd, &pager->freed_pages_count, sizeof(uint32_t));
    if (bytes_read == -1) {
      perror("Error reading freed pages count");
      exit(EXIT_FAILURE);
    }

    // Handle the case where there are freed pages
      // Read the freed pages stack into the freed_pages_stack array
      bytes_read = read(fd, pager->freed_pages_stack, FREED_PAGES_STACK_SIZE);
      if (bytes_read == -1) {
        perror("Error reading freed pages stack");
        exit(EXIT_FAILURE);
      }

    // Calculate file length excluding the freed pages section
    pager->file_length = file_size - FREED_PAGES_STACK_SIZE;

    // Calculate the number of pages based on the file length
    pager->num_pages = pager->file_length / PAGE_SIZE;
  }

  for (uint32_t i = 0; i < MAX_NUM_LOADED_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->page_numbers[i] = -1;
    pager->pinned[i] = false;
  }

  return pager;
}

Table* db_open(const char* filename) {
  PinnedPages* tracker = init_pinned_pages();
  Pager* pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;
  lru_list_initialize(pager);

  if (pager->num_pages == 0) {
   
    // New database file. Initialize page 0 as leaf node.
    char* root_node = get_page(pager, 0, tracker);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  unpin_all_pages(pager, tracker);
  return table;
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read == -1) {
    if (feof(stdin)) {
        printf("End of input reached (EOF)\n");
    } else {
        perror("Error reading input");
        exit(EXIT_FAILURE);
    }
  }

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // Ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

void flush_freed_pages_stack(Pager* pager) {
  off_t offset = lseek(pager->file_descriptor, 0, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, &pager->freed_pages_count, sizeof(uint32_t));

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  offset = lseek(pager->file_descriptor, sizeof(uint32_t), SEEK_SET);

  bytes_written = write(pager->file_descriptor, pager->freed_pages_stack, FREED_PAGES_STACK_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }

}

void db_close(Table* table) {
  Pager* pager = table->pager;

  flush_freed_pages_stack(pager);

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->page_numbers[i] == -1) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[pager->page_numbers[i]]);
    pager->pages[pager->page_numbers[i]] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < MAX_NUM_LOADED_PAGES; i++) {
    char* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }


  free(pager);
  free(table);

}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_INSERT;

  strtok(input_buffer->buffer, " ");
  char* id_string = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_delete(InputBuffer* input_buffer, Statement* statement) {
   statement->type = STATEMENT_DELETE;
   strtok(input_buffer->buffer, " ");
   char* id_string = strtok(NULL, " ");

   if (id_string == NULL) {
    return PREPARE_SYNTAX_ERROR;
   }

   int id = atoi(id_string);
    if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }

  statement->delete_id = id;

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
    return prepare_delete(input_buffer, statement);
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) { 
  if (!is_empty_stack(pager)) {
    return pop_free_page(pager);
  }
  else {
    return pager->num_pages;
  }
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
  PinnedPages* tracker = init_pinned_pages();

  /*
  Handle splitting the root.
  Old root copied to new page, becomes left child.
  Address of right child passed in.
  Re-initialize root page to contain the new root node.
  
  New root node points to two children.
  */

  char* root = get_page(table->pager, table->root_page_num, tracker);
  char* right_child = get_page(table->pager, right_child_page_num, tracker);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  char* left_child = get_page(table->pager, left_child_page_num, tracker);

  if (get_node_type(root) == NODE_INTERNAL) {
    initialize_internal_node(right_child);
    initialize_internal_node(left_child);
  }

  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  if (get_node_type(left_child) == NODE_INTERNAL) {
    char* child;
    for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
      child = get_page(table->pager, *internal_node_child(left_child,i), tracker);
      *node_parent(child) = left_child_page_num;
    }
    child = get_page(table->pager, *internal_node_right_child(left_child), tracker);
    *node_parent(child) = left_child_page_num;
  }

  /* Root node is a new internal node with one key and two children */
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;

  unpin_all_pages(table->pager, tracker);

}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key);

void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {

  PinnedPages* tracker = init_pinned_pages();

  /*
  Add a new child/key pair to parent that corresponds to child
  */

  char* parent = get_page(table->pager, parent_page_num, tracker);
  char* child = get_page(table->pager, child_page_num, tracker);

  uint32_t child_max_key = get_node_max_key(table->pager, child);
  uint32_t index = internal_node_find_child(parent, child_max_key);
  uint32_t original_num_keys = *internal_node_num_keys(parent);

  if (original_num_keys >= INTERNAL_NODE_MAX_KEYS) {
    internal_node_split_and_insert(table, parent_page_num, child_page_num);
    return;
  }
  uint32_t right_child_page_num = *internal_node_right_child(parent);
  /*
  An internal node with a right child of INVALID_PAGE_NUM is empty
  */
  if (right_child_page_num == INVALID_PAGE_NUM) {
    *internal_node_right_child(parent) = child_page_num;
    unpin_all_pages(table->pager, tracker);
    return;
  }

  char* right_child = get_page(table->pager, right_child_page_num, tracker);  
  /*
  If we are already at the max number of cells for a node, we cannot increment
  before splitting. Incrementing without inserting a new key/child pair
  and immediately calling internal_node_split_and_insert has the effect
  of creating a new key at (max_cells + 1) with an uninitialized value
  */
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (child_max_key > get_node_max_key(table->pager, right_child)) {
    /* Replace right child */
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table->pager, right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    /* Make room for the new cell */
    for (uint32_t i = original_num_keys; i > index; i--) {
      char* destination = (char*)internal_node_cell(parent, i);
      char* source = (char*)internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
  unpin_all_pages(table->pager, tracker);
}

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {

  PinnedPages* tracker = init_pinned_pages();

  uint32_t old_page_num = parent_page_num;

  char* old_node = get_page(table->pager, parent_page_num, tracker);
  uint32_t old_max = get_node_max_key(table->pager, old_node);
  char* child = get_page(table->pager, child_page_num, tracker); 
  uint32_t child_max = get_node_max_key(table->pager, child);

  uint32_t new_page_num = get_unused_page_num(table->pager);

  /*
  Declaring a flag before updating pointers which
  records whether this operation involves splitting the root -
  if it does, we will insert our newly created node during
  the step where the table's new root is created. If it does
  not, we have to insert the newly created node into its parent
  after the old node's keys have been transferred over. We are not
  able to do this if the newly created node's parent is not a newly
  initialized root node, because in that case its parent may have existing
  keys aside from our old node which we are splitting. If that is true, we
  need to find a place for our newly created node in its parent, and we
  cannot insert it at the correct index if it does not yet have any keys
  */
  uint32_t splitting_root = is_node_root(old_node);

  char* parent;
  char* new_node;
  if (splitting_root) {
    create_new_root(table, new_page_num);
    parent = get_page(table->pager,table->root_page_num, tracker);
    /*
    If we are splitting the root, we need to update old_node to point
    to the new root's left child, new_page_num will already point to
    the new root's right child
    */
    old_page_num = *internal_node_child(parent,0);

    old_node = get_page(table->pager, old_page_num, tracker);

  } else {
    parent = get_page(table->pager,*node_parent(old_node), tracker);
    new_node = get_page(table->pager, new_page_num, tracker);
    initialize_internal_node(new_node);
  }
  uint32_t* old_num_keys = internal_node_num_keys(old_node);

  uint32_t cur_page_num = *internal_node_right_child(old_node);
  char* cur = get_page(table->pager, cur_page_num, tracker);

  /*
  First put right child into new node and set right child of old node to invalid page number
  */
  internal_node_insert(table, new_page_num, cur_page_num);
  *node_parent(cur) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;

  /*
  For each key until you get to the middle key, move the key and the child to the new node
  */
  for (int i = INTERNAL_NODE_MAX_KEYS - 1; i 
    > INTERNAL_NODE_MAX_KEYS / 2; i--) {
    cur_page_num = *internal_node_child(old_node, i);
    cur = get_page(table->pager, cur_page_num, tracker);
    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;
    (*old_num_keys)--;
  }

  /*
  Set child before middle key, which is now the highest key, to be node's right child,
  and decrement number of keys
  */
  *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
  (*old_num_keys)--;

  /*
  Determine which of the two nodes after the split should contain the child to be inserted,
  and insert the child
  */
  uint32_t max_after_split = get_node_max_key(table->pager, old_node);

  uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;
  update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));
  if (!splitting_root) {
    internal_node_insert(table,*node_parent(old_node),new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
  unpin_all_pages(table->pager, tracker);
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {

  PinnedPages* tracker = init_pinned_pages();

  /*
  Create a new node and move half the cells over.
  Insert the new value in one of the two nodes.
  Update parent or create a new parent.
 
  */
  char* old_node = get_page(cursor->table->pager, cursor->page_num, tracker);
  uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  char* new_node = get_page(cursor->table->pager, new_page_num, tracker);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  /*
  All existing keys plus new key should should be divided
  evenly between old (left) and new (right) nodes.
  Starting from the right, move each key to correct position.
  */
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    char* destination_node;
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    char* destination = leaf_node_cell(destination_node, index_within_node);

    if (i == cursor->cell_num) {
      serialize_row(value,
                    leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }

  /* Update cell count on both leaf nodes */
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    create_new_root(cursor->table, new_page_num);
    unpin_all_pages(cursor->table->pager, tracker);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);

    uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
    char* parent = get_page(cursor->table->pager, parent_page_num, tracker);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    unpin_all_pages(cursor->table->pager, tracker);
    return;
  }
}


void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {

  PinnedPages* tracker = init_pinned_pages();

  char* node = get_page(cursor->table->pager, cursor->page_num, tracker);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // Node full
    leaf_node_split_and_insert(cursor, key, value);
    unpin_all_pages(cursor->table->pager, tracker);
    return;
  }

  if (cursor->cell_num < num_cells) {
    // Make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    } 
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
  unpin_all_pages(cursor->table->pager, tracker);
}

void internal_node_merge(Table* table, uint32_t page_num);

void internal_node_delete(Table* table, uint32_t parent_page_num, uint32_t child_page_num, uint32_t index) {

  PinnedPages* tracker = init_pinned_pages();

  /* Fetch the child to be deleted and its internal node. */

  char* child = get_page(table->pager, child_page_num, tracker);
  char* parent = get_page(table->pager, parent_page_num, tracker);

    /* If the child is the right child of its parent, we update the parent's right child pointer to the child directly left of the right child. */
    if (index == *internal_node_num_keys(parent)) {
      *internal_node_right_child(parent) = *internal_node_child(parent, index - 1);
    }
      
    /* If the child is not the right child of its parent, we shift all the children after the child's position in the parent one step to the left.*/
      
    else {
      for (uint32_t i = index; i + 1 < *internal_node_num_keys(parent); i++) {
        memcpy((char*)internal_node_cell(parent, i), (char*)internal_node_cell(parent, i + 1), INTERNAL_NODE_CELL_SIZE);
      }
    }
    /* If the child was the right child of its parent, then we also need to update its key in its parent and
      so on if the parent is the right child of its own parent. */
  
    if (index == *internal_node_num_keys(parent)) {
      uint32_t old_max_key = get_node_max_key(table->pager, child);
      uint32_t new_max_key = get_node_max_key(table->pager, get_page(table->pager, *internal_node_right_child(parent), tracker));
      while (*internal_node_right_child(parent) == parent_page_num && !is_node_root(parent)) {
        parent_page_num = *node_parent(get_page(table->pager, parent_page_num, tracker));
        parent = get_page(table->pager, *node_parent(parent), tracker);
        }
      update_internal_node_key(parent, old_max_key, new_max_key);
    }
    /* Decrement the number of children in the parent by 1 */
    parent_page_num = *node_parent(child);
    *internal_node_num_keys(parent) -= 1;
    push_free_page(table->pager, child_page_num);

    /* If the parent becomes underfilled after its child was deleted, call internal_node_merge()*/
    if (*internal_node_num_keys(parent) < 1 && !is_node_root(parent)) {
      internal_node_merge(table, parent_page_num);
    }

    unpin_all_pages(table->pager, tracker);
    
}

void internal_node_merge(Table* table, uint32_t page_num) {
  PinnedPages* tracker = init_pinned_pages();
  
  /* Fetch the underfilled internal node, its parent, its right child, and its index within its own parent.*/
  
  char* node = get_page(table->pager, page_num, tracker);
  char* parent = get_page(table->pager, *node_parent(node), tracker);
  char* child = get_page(table->pager, *internal_node_right_child(node), tracker);
  uint32_t index = internal_node_find_child(parent, get_node_max_key(table->pager, node)); 
  
  /* Initialize the underfilled internal node's sibling to an invalid page number and 
  initialize index and cell number to 0. */
  
  uint32_t sibling_page_num = INVALID_PAGE_NUM;
  uint32_t sibling_index = 0;
  uint32_t sibling_cell_num = 0;
  
  /* Initialize the underfilled internal node's sibling's index to the index of the node directly to the left of it if the underfilled node
  is the right child of its parent. Otherwise, initialize  underfilledinternal node's sibling's index to the index of the node directly to 
  the right of it.*/
  
  if (index == *internal_node_num_keys(parent)) {
    sibling_index = index - 1;
  }
  else {
    sibling_index = index + 1;
  }
  sibling_page_num = *internal_node_child(parent, sibling_index);

  char* sibling = get_page(table->pager, sibling_page_num, tracker);

  /* If the underfilled internal node's sibling has less more than child, we transfer a child from it to 
  the underfilled internal node to help fill the underfilled internal node. */
  
  if (*internal_node_num_keys(sibling) > 1) {
    
    /*If the underfilled internal node's sibling is the index of the node directly to the left of the 
    underfilled internal node, we initialize the index of the underfilled internal node's sibling's 
    child to index of the underfilled internal node's sibling's right child. If the underfilled internal 
    node's sibling is the index of the node directly to the right of the underfilled internal node, we 
    initialize the index of the underfilled internal node's sibling's child to index of the underfilled 
    internal node's sibling's first child */
    
    if (sibling_index == index - 1) {
      sibling_cell_num = *internal_node_num_keys(sibling);
    }
    char* source = get_page(table->pager, *internal_node_child(sibling, sibling_cell_num), tracker);
    
    /* Set the underfilled internal node's sibling's child's parent to be the underfilled internal node and increase 
    the underfilled internal node's number of children by 1. */
    
    *node_parent(source) = page_num;
    *internal_node_num_keys(node) += 1;

    /* If the maximum key of the underfilled internal node is less than the maximum key of the underfilled internal node's 
    sibling's child's maximum key, we need to update the underfilled internal node's maximum key to be the underfilled internal 
    node's sibling's child's maximum key */
    
    if (get_node_max_key(table->pager, source) > get_node_max_key(table->pager, child)) {
      *internal_node_child(node, 0) = *internal_node_right_child(node);
      *internal_node_key(node, 0) = get_node_max_key(table->pager, child);
      *internal_node_right_child(node) = *internal_node_child(sibling, sibling_cell_num);

      uint32_t old_max_key = get_node_max_key(table->pager, child);
      uint32_t new_max_key = get_node_max_key(table->pager, source); 
      
      /* If the underfilled internal node was the right child of its parent, then we also need to update its key in its parent and
      so on if the parent is the right child of its own parent. */
      
      if (*internal_node_right_child(parent) == page_num) {
        uint32_t parent_page_num = *node_parent(node);
        parent = get_page(table->pager, *node_parent(parent), tracker);
        while (*internal_node_right_child(parent) == parent_page_num && !is_node_root(parent)) {
          parent_page_num = *node_parent(get_page(table->pager, parent_page_num, tracker));
          parent = get_page(table->pager, *node_parent(parent), tracker);
        }
      }
      update_internal_node_key(parent, old_max_key, new_max_key);
    }
      
    /* Set the first child of the underfilled internal node to the first child of the underfilled internal node's sibling. */
      
    else {
      *internal_node_child(node, 0) = *internal_node_child(sibling, sibling_cell_num);
      *internal_node_key(node, 0) = get_node_max_key(table->pager, source);
    }
    /* Delete from the underfilled internal node's sibling its child that was transferred to the underfilled internla node */
    
    internal_node_delete(table, sibling_page_num, *internal_node_child(sibling, sibling_cell_num), sibling_cell_num);
    pop_free_page(table->pager);
  }
  /* If the underfilled internal node's sibling has only 1 child and the parent of the underfilled internal node is the root, 
  we transfer the the underfilled internal node's only child to the underfilled node's sibling and set the sibling as the root.*/
    
  else if (*internal_node_num_keys(sibling) == 1) {
    if (*internal_node_num_keys(parent) == 1 && is_node_root(parent)) {
      internal_node_insert(table, sibling_page_num, *internal_node_right_child(node));
  
      for (uint32_t i = 0; i < *internal_node_num_keys(sibling) + 1; i++) {
        *node_parent(get_page(table->pager, *internal_node_child(sibling, i), tracker)) = *node_parent(node);
      }
      memcpy(parent, sibling, PAGE_SIZE);
      set_node_root(parent, true);

      push_free_page(table->pager, sibling_page_num);
    }

      /* If the underfilled internal node's sibling has only 1 child and the parent of the underfilled internal node is not the root, 
      we transfer the the underfilled internal node's only child to the underfilled node's sibling and delete the underfilled internal node
      from its parent.*/
      
    else {
      internal_node_insert(table, sibling_page_num, *internal_node_right_child(node));
      *node_parent(child) = sibling_page_num;
      internal_node_delete(table, *node_parent(node), page_num, index);
    }
  }
  unpin_all_pages(table->pager, tracker);
}

void leaf_node_merge(Cursor* cursor);

void leaf_node_delete(Cursor* cursor, uint32_t key) {

  PinnedPages* tracker = init_pinned_pages();
  
  /* Fetch the leaf node of the row that is to be deleted and how many rows it currently ha.s */
  
  char* node = get_page(cursor->table->pager, cursor->page_num, tracker);
  uint32_t num_cells = *leaf_node_num_cells(node);

  /* If the row to be deleted is not the right child of its leaf node, we shift all the rows 
  after the deleted one to the left to fill the gap. */
  
  if (cursor->cell_num + 1 < num_cells) {
    for (uint32_t i = cursor->cell_num; i + 1 < num_cells; i++) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i + 1), LEAF_NODE_CELL_SIZE);
    }
  }
    
  /* If the row to be deleted is the right child of its leaf node, we decrease the number of rows its leaf node
  has by 1. */
    
  else if (cursor->cell_num + 1 == num_cells && !is_node_root(node)) {
    
    /* If the leaf node containing the row to be deleted is not the root, then we must also update the leaf node's key in its own parent 
    and so on if the parent is the right child of its own parent.*/
    
    char* parent = get_page(cursor->table->pager, *node_parent(node), tracker);
    uint32_t old_max_key = get_node_max_key(cursor->table->pager, node);
    uint32_t new_max_key = *leaf_node_key(node, cursor->cell_num - 1);
    uint32_t index = internal_node_find_child(parent, get_node_max_key(cursor->table->pager, node));
    if (index == *internal_node_num_keys(parent) && !is_node_root(parent)) {
      uint32_t parent_page_num = *node_parent(node);
      parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
      while (*internal_node_right_child(parent) == parent_page_num && !is_node_root(parent)) {
        parent_page_num = *node_parent(get_page(cursor->table->pager, parent_page_num, tracker));
        parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
      }
    }
    update_internal_node_key(parent, old_max_key, new_max_key);
  }

  *(leaf_node_num_cells(node)) -= 1;
  
  /* If the leaf node that of the deleted row becomes underfilled after deletion, and it is not the root, we call leaf_node_merge*/
  
  if (*leaf_node_num_cells(node) < 7 && !is_node_root(node)) {
    leaf_node_merge(cursor);
  }
  unpin_all_pages(cursor->table->pager, tracker);
}


void leaf_node_merge(Cursor* cursor) {

  PinnedPages* tracker = init_pinned_pages();

  /* For our underfilled leaf node, we will need to find assign either the node to its left as its sibling
  if it is the right child. Otherwise, we can just assign the node to the right as the sibling. 

  If the sibling has more than 7 rows, we can safely take a row from it to give to the underfilled road. And 
  then update the node's max key in the parent internal node.

  However, if the sibling has 7 rows, we insert the underfilled node's rows into the sibling. If, as a result 
  of this merging, the parent internal node becomes underfilled, we check if the parent internal is the root or not.
  If it is, then we can simply assign our new sibling node as the root. If it is not, then we insert the underfilled 
  leaf node's rows into its sibling node. And then update the next leaf pointers for the sibling and the previous node.
  We then delete the underfilled node from its parent.
  */

  char* node = get_page(cursor->table->pager, cursor->page_num, tracker);
  uint32_t node_max_key =get_node_max_key(cursor->table->pager, node);
  char* parent = get_page(cursor->table->pager, *node_parent(node), tracker);
  uint32_t index = internal_node_find_child(parent, get_node_max_key(cursor->table->pager, node));
  uint32_t sibling_page_num = INVALID_PAGE_NUM;
  uint32_t sibling_index = 0;
  uint32_t sibling_cell_num = 0;

  if (index == *internal_node_num_keys(parent)) {
    sibling_index = index - 1;
  }
  else {
    sibling_index = index + 1;
  }
  sibling_page_num = *internal_node_child(parent, sibling_index);

  char* sibling = get_page(cursor->table->pager, sibling_page_num, tracker);
  uint32_t old_max_key = get_node_max_key(cursor->table->pager, sibling);

  if (*leaf_node_num_cells(sibling) > 7) {
    Row* value = malloc(sizeof(Row));

    if (sibling_index == index - 1) {
      sibling_cell_num = *leaf_node_num_cells(sibling) - 1;
    }

    uint32_t key = *leaf_node_key(sibling, sibling_cell_num);
    deserialize_row(leaf_node_value(sibling, sibling_cell_num), value);

    Cursor* alternate_cursor = leaf_node_find(cursor->table, cursor->page_num, key);
    old_max_key = get_node_max_key(cursor->table->pager, node);
    leaf_node_insert(alternate_cursor, key, value);
    uint32_t new_max_key = get_node_max_key(cursor->table->pager, node);

    if (index == *internal_node_num_keys(parent) && !is_node_root(parent)) { 
        uint32_t parent_page_num = *node_parent(node);
        parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
        while (*internal_node_right_child(parent) == parent_page_num && !is_node_root(parent)) {
          parent_page_num = *node_parent(get_page(cursor->table->pager, parent_page_num, tracker));
          parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
        }
    }

    update_internal_node_key(parent, old_max_key, new_max_key);

    Cursor* sibling_cursor = leaf_node_find(cursor->table, sibling_page_num, key);
    leaf_node_delete(sibling_cursor, key);

    free(value);
    value = NULL;
  }
  else if (*leaf_node_num_cells(sibling) == 7) {
    Row* value = malloc(sizeof(Row));
    for (uint32_t i = 0; i < *leaf_node_num_cells(node); i++) {
      uint32_t key = *leaf_node_key(node, i);
      deserialize_row(leaf_node_value(node, i), value);
      Cursor* sibling_cursor = leaf_node_find(cursor->table, sibling_page_num, key);
      leaf_node_insert(sibling_cursor, key, value);
    }

    free(value);
    value = NULL;

    if (*internal_node_num_keys(parent) == 1 && is_node_root(parent)) {
      memcpy(parent, sibling, PAGE_SIZE);
      set_node_type(parent, NODE_LEAF);
      set_node_root(parent, true);
      *leaf_node_next_leaf(parent) = 0;
      push_free_page(cursor->table->pager, sibling_page_num);
      push_free_page(cursor->table->pager, cursor->page_num);
    }
    else {
      if (index == *internal_node_num_keys(parent) && !is_node_root(parent)) {
        uint32_t parent_page_num = *node_parent(node);
        parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
        while (*internal_node_right_child(parent) == parent_page_num && !is_node_root(parent)) {
          parent_page_num = *node_parent(get_page(cursor->table->pager, parent_page_num, tracker));
          parent = get_page(cursor->table->pager, *node_parent(parent), tracker);
        }
      }
      uint32_t new_max_key = get_node_max_key(cursor->table->pager, sibling);
      update_internal_node_key(parent, old_max_key, new_max_key);
      parent = get_page(cursor->table->pager, *node_parent(node), tracker);
      
      if (index == *internal_node_num_keys(parent) ) {
        *leaf_node_next_leaf(sibling) = *leaf_node_next_leaf(node);
      }
      else {
        Cursor* prev_cursor = table_start(cursor->table);
        char* prev_leaf_node = get_page(cursor->table->pager, prev_cursor->page_num, tracker);
        while (*leaf_node_next_leaf(prev_leaf_node) != cursor->page_num && prev_cursor->page_num != cursor->page_num) {
          prev_cursor->page_num = *leaf_node_next_leaf(prev_leaf_node);
          prev_leaf_node = get_page(cursor->table->pager, prev_cursor->page_num, tracker);
        }
        if (prev_cursor->page_num != cursor->page_num) {
          *leaf_node_next_leaf(prev_leaf_node) = sibling_page_num;
        }
      }

      internal_node_delete(cursor->table, *node_parent(node), cursor->page_num, index);
    }
  }
  unpin_all_pages(cursor->table->pager, tracker);
}

ExecuteResult execute_insert(Statement* statement, Table* table) {

  PinnedPages* tracker = init_pinned_pages();

  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;

  Cursor* cursor = table_find(table, key_to_insert);
  char* node = get_page(table->pager, cursor->page_num, tracker);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  unpin_all_pages(cursor->table->pager, tracker);
  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement* statement, Table* table) {

  PinnedPages* tracker = init_pinned_pages();

  uint32_t key_to_delete = statement->delete_id;

  Cursor* cursor = table_find(table, key_to_delete);
  char* node = get_page(table->pager, cursor->page_num, tracker);

  uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);

  if (key_at_index != key_to_delete) {
    return EXECUTE_KEY_NOT_FOUND;
  }
  leaf_node_delete(cursor, key_to_delete);

  unpin_all_pages(table->pager, tracker);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
    case (STATEMENT_DELETE):
      return execute_delete(statement, table);
    }
  return EXECUTE_FAIL;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command: '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_NEGATIVE_ID):
        printf("ID must be positive.\n");
        continue;
      case (PREPARE_STRING_TOO_LONG):
        printf("String is too long.\n");
        continue;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error. Could not parse statement.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_DUPLICATE_KEY):
        printf("Error: Duplicate key.\n");
        break;
      case (EXECUTE_KEY_NOT_FOUND):
        printf("Error: Key not found.\n");
        break;
      case(EXECUTE_FAIL):
        printf("Error: Failed to execute.\n");
        break;
    }
  }
}
