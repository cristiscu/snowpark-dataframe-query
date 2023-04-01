Snowpark DataFrame Query
========================

Generate a rather complex SQL query with the DataFrame class from Snowpark for Python.

# Database Profile File

Create a **profiles_db.conf** copy of the **profiles_db_template.conf** file, and customize it with your own Snowflake connection parameters: the account name, the user name, the role and warehouse. Your top [default] profile is the active profile, considered by our tool. Below you may define other personal profiles, that you may override under [default] each time you want to change your active connection.

Use **SNOWFLAKE_SAMPLE_DATA** for your database, and **TPCDS_SF10TCL** for your schema. This shared database should be shared in every new Snowflake account.

We connect to Snowflake with the Snowflake Connector for Python. We have code for (a) password-based connection, (b) connecting with a Key Pair, and (c) connecting with SSO. For password-based connection, save your password in a SNOWFLAKE_PASSWORD local environment variable. Never add the password or any other sensitive information to your code or to profile files. All names must be case sensitive, with no quotes.

# CLI Executable File

To compile into a CLI executable:

**<code>pip install pyinstaller</code>**  
**<code>pyinstaller --onefile snowpark-dataframe-query.py</code>**  
**<code>dist\snowpark-dataframe-query</code>**  

# Generate and Run a Complex Schema with DataFrame

Run this code and check the executed query in Snowflake's History tab.

**<code>python snowpark-dataframe-query</code>**  
