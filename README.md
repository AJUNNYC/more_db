# Enhanced DB_Tutorial Project

This project builds upon the original **db_tutorial** project, adding several important features to improve its functionality and efficiency. The enhancements include a **delete** feature, a **mechanism for freeing pages**, an **LRU (Least Recently Used) mechanism** for loading and unloading pages, and a transition to using `char*` instead of the previous `void*` for better consistency.

## Table of Contents
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Description

This project is an extension of the **db_tutorial**, a simple database project. The enhancements in this version aim to provide better memory management, efficient page handling, and consistent pointer usage. The project now includes:

- **Delete functionality**: The ability to delete records from the database.
- **Memory management improvements**: A new mechanism for freeing pages to optimize memory usage.
- **LRU mechanism**: A cache management strategy to load and unload pages based on usage, improving performance.
- **Pointer consistency**: Switching from `void*` to `char*` to make pointer usage more consistent throughout the codebase.

## Features

- **Delete Operation**: Implemented a mechanism to delete data entries from the database.
- **Page Management**: Introduced a page freeing mechanism that efficiently handles memory.
- **LRU Caching**: Uses an LRU caching system to automatically manage page loading/unloading, keeping the most recently accessed pages in memory.
- **Code Cleanup**: Replaced `void*` with `char*` for better type safety and consistency in pointer usage across the codebase.

## Installation

### Steps:
1. Download the C file (`db_tutorial_enhanced.c`) to your local machine.

2. Compile the file using `clang`:
   ```bash
   clang -o db_tutorial_enhanced db_tutorial_enhanced.c

## USAGE
You can use this project as a simple database that allows inserting, deleting, and printing the structure of the tree. Here's an example of how to interact with the program:
```
db > insert 1 user1 person1@example.com
db > insert 2 user2 person2@example.com
db > insert 3 user3 person3@example.com
db > delete 2
db > .btree
db > .exit
```

- **insert**: Adds a new record to the database. Each insert command will add a user record with an ID, name, and email address.
- **delete**: Removes a record from the database by specifying the ID of the user to be deleted.
- **.btree**: Prints the current structure of the B-tree. The B-tree will show the keys and how they are distributed across the internal and leaf nodes.
- **.exit**: Exits the program.

### Example Walkthrough

#### Inserting Data:

To insert users, run commands like the following:

```
db > insert 1 user1 person1@example.com
db > insert 2 user2 person2@example.com
db > insert 3 user3 person3@example.com
```
This will insert users with IDs 1, 2, and 3 into the database.

#### Deleting Data:

To delete a user from the database, you can use the `delete` command. For example:

```
db > delete 2
```
This will delete the user with ID 2 from the database.

#### Viewing the B-tree Structure:

To see the current structure of the B-tree, use the `.btree` command:

```
db > .btree
```
This will display a hierarchical structure of the B-tree with nodes and keys.

#### Example Output:

```
db > Tree:
- internal (size 1)
  - leaf (size 2)
    - 1
    - 3
  - key 3
```

#### Exiting the Program:

To exit the program, use the .exit command:
```
db > .exit
```

### Delete Command

The `delete` command allows you to remove a record from the database by specifying its unique ID. For example:

```
db > delete 4
```

This will remove the record with ID 4 from the database and update the B-tree accordingly.

