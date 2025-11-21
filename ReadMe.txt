Walmart Near-Real-Time Data Warehouse - README
Prerequisites
Python 3.8 or higher installed
MySQL 8.0 or higher installed
Required Python libraries: pandas, mysql-connector-python

 Step-by-Step Instructions

 Step 1: Organize Project Files

Place all files in the same directory:
Important: All CSV files and Python file must be in the same folder.

 Step 2: Start MySQL Server
Make sure your MySQL server is running:

 Step 3: Create Database
Open MySQL Workbench or MySQL command line and execute the SQL script:

MySQL Workbench
1. Open MySQL Workbench
2. Connect to your local server
3. File → Open SQL Script → Select schema.sql
4. Click Execute 

 Step 4: Get Database Credentials

Open MySQL Workbench and note your connection details:

1. Click "Database" → "Manage Connections"
2. Select your connection (usually "Local instance")
3. Note the following:
   - Hostname: Usually 127.0.0.1 or localhost
   - Username: Usually root
   - Password: Your MySQL password

 Step 5: Run the ETL Process
Open terminal/command prompt in your project directory and run:

 Step 6: Enter Database Credentials
The program will prompt you for:
Database Configuration:
   Enter MySQL host [127.0.0.1]: 
   Enter MySQL username [root]: 
   Enter MySQL password: 

- Host: Press Enter for default 127.0.0.1, or type your hostname
- Username: Press Enter for default root, or type your username  
- Password: Type your MySQL password (won't show on screen - this is normal)
