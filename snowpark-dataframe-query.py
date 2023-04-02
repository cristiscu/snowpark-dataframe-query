"""
Created By:    Cristian Scutaru
Creation Date: Mar 2023
Company:       XtractPro Software
"""

import os, configparser
from snowflake.snowpark import Session, Window
from snowflake.snowpark.functions import sum, avg, rank, abs

def makeV1(session):
    # (a) the tables
    item = session.table("item") \
        .select("i_category", "i_brand", "i_item_sk")
    catalog_sales = session.table("catalog_sales") \
        .select("cs_item_sk", "cs_call_center_sk",
            "cs_sold_date_sk", "cs_sales_price")
    date_dim = session.table("date_dim") \
        .select("d_year", "d_moy", "d_date_sk")
    call_center = session.table("call_center") \
        .select("cc_name", "cc_call_center_sk")

    # (b) the FROM clause, with all JOINs
    from1 = catalog_sales \
        .join(item, \
            catalog_sales.cs_item_sk == item.i_item_sk) \
        .join(date_dim, \
            catalog_sales.cs_sold_date_sk == date_dim.d_date_sk) \
        .join(call_center, \
            catalog_sales.cs_call_center_sk == call_center.cc_call_center_sk)

    # (c) the WHERE clause
    filter1 = from1.filter((from1.d_year == 1999)
        | ((from1.d_year == (1999 - 1)) & (from1.d_moy == 12))
        | ((from1.d_year == (1999 + 1)) & (from1.d_moy == 1)))

    # (d) all OVER clauses
    avg_monthly_sales_window = Window \
        .partitionBy("i_category", "i_brand", "cc_name", "d_year")
    rank_window = Window \
        .partitionBy("i_category", "i_brand", "cc_name") \
        .orderBy("d_year", "d_moy")

    # (e) the GROUP BY clause
    group1 = filter1.groupBy(
        "i_category", "i_brand", "cc_name", "d_year", "d_moy")

    # (f) selection of aggregate functions
    agg1 = group1.agg(
        sum("cs_sales_price") \
            .alias("sum_sales"),
        avg(sum("cs_sales_price")) \
            .over(avg_monthly_sales_window) \
            .alias("avg_monthly_sales"),
        rank() \
            .over(rank_window) \
            .alias("rn"))

    return agg1

def makeV2(v1):
    # (a) reuse the same CTE
    v1_lag = v1.select(
        v1.i_category.alias("lag_category"),
        v1.i_brand.alias("lag_brand"),
        v1.cc_name.alias("lag_name"),
        v1.rn.alias("lag_rn"),
        v1.sum_sales.alias("psum"))
    v1_lead = v1.select(
        v1.i_category.alias("lead_category"),
        v1.i_brand.alias("lead_brand"),
        v1.cc_name.alias("lead_name"),
        v1.rn.alias("lead_rn"),
        v1.sum_sales.alias("nsum"))

    # (b) the FROM clause with all the JOINs
    v11 = v1.join(v1_lag,
        (v1.i_category == v1_lag.lag_category)
            & (v1.i_brand == v1_lag.lag_brand)
            & (v1.cc_name == v1_lag.lag_name)
            & (v1.rn == v1_lag.lag_rn + 1))
    from2 = v11.join(v1_lead,
        (v11.i_category == v1_lead.lead_category)
            & (v11.i_brand == v1_lead.lead_brand)
            & (v11.cc_name == v1_lead.lead_name)
            & (v11.rn == v1_lead.lead_rn - 1))

    # (c) the SELECT clause
    select2 = from2.select("i_category", "d_year", "d_moy",
        "avg_monthly_sales", "sum_sales", "psum", "nsum")

    return select2

def makeFinal(v2):
    # (a) the WHERE clause
    filter3 = v2.filter((v2.d_year == 1999)
        & (v2.avg_monthly_sales > 0)
        & ((abs(v2.sum_sales - v2.avg_monthly_sales)
            / v2.avg_monthly_sales) > 0.1))

    # (b) the ORDER BY clause
    order3 = filter3.sort(
        v2["sum_sales"] - v2["avg_monthly_sales"],
        v2["d_moy"])

    # (c) the LIMIT clause
    limit3 = order3.limit(10)

    return limit3

def main():
    """
    Main entry point of the CLI
    """

    # read profiles_db.conf
    parser = configparser.ConfigParser()
    parser.read("profiles_db.conf")
    section = "default"

    pars = {
        "account": parser.get(section, "account"),
        "user": parser.get(section, "user"),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "role": parser.get(section, "role"),
        "warehouse": parser.get(section, "warehouse"),
        "database": parser.get(section, "database"),
        "schema": parser.get(section, "schema")
    }

    # create a Snowpark session, build and run a complex query
    with Session.builder.configs(pars).create() as session:
        makeFinal(makeV2(makeV1(session))).show()

if __name__ == "__main__":
    main()
