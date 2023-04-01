"""
Created By:    Cristian Scutaru
Creation Date: Mar 2023
Company:       XtractPro Software
"""

import os, configparser
from snowflake.snowpark import Session, Window
from snowflake.snowpark.functions import col, sum, avg, rank, abs

def makeV1(session):
    # (a) the tables
    item = session.table("item"
        ).select(col("i_category"), col("i_brand"), col("i_item_sk"))
    catalog_sales = session.table("catalog_sales"
        ).select(col("cs_item_sk"), col("cs_call_center_sk"),
            col("cs_sold_date_sk"), col("cs_sales_price"))
    date_dim = session.table("date_dim"
        ).select(col("d_year"), col("d_moy"), col("d_date_sk"))
    call_center = session.table("call_center"
        ).select(col("cc_name"), col("cc_call_center_sk"))

    # (b) the FROM clause, with all JOINs
    from1 = catalog_sales.join(item,
             catalog_sales["cs_item_sk"] == item["i_item_sk"]
        ).join(date_dim,
            catalog_sales["cs_sold_date_sk"] == date_dim["d_date_sk"]
        ).join(call_center,
               catalog_sales["cs_call_center_sk"] == call_center["cc_call_center_sk"])

    # (c) the WHERE clause
    filter1 = from1.filter((col("d_year") == 1999)
        | ((col("d_year") == (1999 - 1)) & (col("d_moy") == 12))
        | ((col("d_year") == (1999 + 1)) & (col("d_moy") == 1)))

    # (d) all OVER clauses
    avg_monthly_sales_window = Window.partitionBy(
        col("i_category"), col("i_brand"),
        col("cc_name"), col("d_year"))
    rank_window = Window.partitionBy(
            col("i_category"), col("i_brand"), col("cc_name")
        ).orderBy(col("d_year"), col("d_moy"))

    # (e) the GROUP BY clause
    group1 = filter1.groupBy(
        col("i_category"), col("i_brand"), col("cc_name"),
        col("d_year"), col("d_moy"))

    # (f) selection of aggregate functions
    agg1 = group1.agg(
        sum(col("cs_sales_price")).alias("sum_sales"),
        avg(sum(col("cs_sales_price"))
            ).over(avg_monthly_sales_window
            ).alias("avg_monthly_sales"),
        rank().over(rank_window).alias("rn"))

    return agg1

def makeV2(v1):
    # (a) reuse the same CTE
    v1_lag = v1.toDF([
            "i_category", "i_brand", "cc_name", "d_year", "d_moy",
            "sum_sales", "avg_monthly_sales", "rn"
        ]).select(col("i_category").alias("lag_category"),
            col("i_brand").alias("lag_brand"),
            col("cc_name").alias("lag_name"),
            col("rn").alias("lag_rn"),
            col("sum_sales").alias("psum"))
    v1_lead = v1.toDF([
            "i_category", "i_brand", "cc_name", "d_year", "d_moy",
            "sum_sales", "avg_monthly_sales", "rn"
        ]).select(col("i_category").alias("lead_category"),
            col("i_brand").alias("lead_brand"),
            col("cc_name").alias("lead_name"),
            col("rn").alias("lead_rn"),
            col("sum_sales").alias("nsum"))

    # (b) the FROM clause with all the JOINs
    from2 = v1.join(v1_lag,
            (v1["i_category"] == v1_lag["lag_category"])
                & (v1["i_brand"] == v1_lag["lag_brand"])
                & (v1["cc_name"] == v1_lag["lag_name"])
                & (v1["rn"] == v1_lag["lag_rn"] + 1)
        ).join(v1_lead,
            (v1["i_category"] == v1_lead["lead_category"])
                & (v1["i_brand"] == v1_lead["lead_brand"])
                & (v1["cc_name"] == v1_lead["lead_name"])
                & (v1["rn"] == v1_lead["lead_rn"] - 1))

    # (c) the SELECT clause
    select2 = from2.select(
        v1.col("i_category"), v1.col("d_year"), v1.col("d_moy"),
        v1.col("avg_monthly_sales"), v1.col("sum_sales"),
        v1_lag.col("psum"), v1_lead.col("nsum"))

    return select2

def makeFinal(v2):
    # (a) the WHERE clause
    filter3 = v2.filter(
        (col("d_year") == 1999)
            & (col("avg_monthly_sales") > 0)
            & ((abs(col("sum_sales") - col("avg_monthly_sales"))
                / col("avg_monthly_sales")) > 0.1))

    # (b) the ORDER BY clause
    order3 = filter3.sort(
        col("sum_sales") - col("avg_monthly_sales"),
        col("d_moy"))

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
