Enhanced DB_Tutorial Project
This project builds upon the original db_tutorial project, adding several important features to improve its functionality and efficiency. The enhancements include a delete feature, a mechanism for freeing pages, an LRU (Least Recently Used) mechanism for loading and unloading pages, and a transition to using char* instead of the previous void* for better consistency.

Table of Contents
Description
Features
Installation
Usage
Contributing
License
Acknowledgments
Description
This project is an extension of the db_tutorial, a simple database project. The enhancements in this version aim to provide better memory management, efficient page handling, and consistent pointer usage. The project now includes:

Delete functionality: The ability to delete records from the database.
Memory management improvements: A new mechanism for freeing pages to optimize memory usage.
LRU mechanism: A cache management strategy to load and unload pages based on usage, improving performance.
Pointer consistency: Switching from void* to char* to make pointer usage more consistent throughout the codebase.
Features
Delete Operation: Implemented a mechanism to delete data entries from the database.
Page Management: Introduced a page freeing mechanism that efficiently handles memory.
LRU Caching: Uses an LRU caching system to automatically manage page loading/unloading, keeping the most recently accessed pages in memory.
Code Cleanup: Replaced void* with char* for better type safety and consistency in pointer usage across the codebase.
Installation
Prerequisites:
C compiler (e.g., GCC)
Make (for building the project)
Steps:
Clone the repository:

bash
Copy code
git clone https://github.com/yourusername/db_tutorial_enhanced.git
Navigate to the project directory:

bash
Copy code
cd db_tutorial_enhanced
Build the project using make:

bash
Copy code
make
Run the project:

bash
Copy code
./db_tutorial_enhanced
Usage
Insert Data:
To insert data into the database, use the insert command:

bash
Copy code
insert <id> <username> <email>
Example:

bash
Copy code
insert 1 user1 user1@example.com
Delete Data:
To delete a record, use the delete command with the record's ID:

bash
Copy code
delete <id>
Example:

bash
Copy code
delete 1
View Database Structure:
To view the current structure of the database, use the .btree command:

bash
Copy code
.btree
Exit:
To exit the program, use the .exit command:

bash
Copy code
.exit
Contributing
We welcome contributions to this project! If you'd like to contribute, please follow these steps:

Fork the repository.
Create a new branch for your feature or bug fix.
Make your changes and write tests if applicable.
Commit your changes and push to your fork.
Submit a pull request with a description of what you’ve done.
Please make sure to follow the coding guidelines and run all tests before submitting your pull request.

License
This project is licensed under the MIT License - see the LICENSE file for details.

Acknowledgments
db_tutorial: This project is built upon the original db_tutorial project, which provided the foundational structure.
Special thanks to the contributors and authors of the original project for their work and inspiration.# more_db
