# https://docs.snowflake.com/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Table.merge.html
# https://docs.snowflake.com/en/sql-reference/sql/merge
# https://medium.com/p/6c09d0361ab8/edit

import os, configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import when_matched, when_not_matched

parser = configparser.ConfigParser()
parser.read("../profiles_db.conf")
section = "tests"

pars = {
   "account": parser.get(section, "account"),
   "user": parser.get(section, "user"),
   "password": os.getenv('SNOWFLAKE_PASSWORD'),
   "role": parser.get(section, "role"),
   "warehouse": parser.get(section, "warehouse"),
   "database": parser.get(section, "database"),
   "schema": parser.get(section, "schema")
}

session = Session.builder.configs(pars).create()

source = session.create_dataframe(
   [(10, "new"), (12, "new"), (13, "old")],
   schema=["key", "value"])

session.create_dataframe(
      [(10, "old"), (10, "too_old"), (11, "old")],
      schema=["key", "value"]
   ).write.save_as_table(
      "my_table", mode="overwrite", table_type="temporary")
target = session.table("my_table")

target.merge(source,
   (target.key == source.key) & (target.value == "too_old"),
   [when_matched().update({"value": source["value"]}),
   when_not_matched().insert({"key": source["key"]})])
print(target.collect())

session.close()