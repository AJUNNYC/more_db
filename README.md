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

### Prerequisites:
- C compiler (e.g., GCC)
- Make (for building the project)

### Steps:
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/db_tutorial_enhanced.git
