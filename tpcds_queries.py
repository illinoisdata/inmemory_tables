from ExecutionNode import *
from utils import *
import polars as pl
import numpy as np

"""
A file containing select TPC-DS queries translated into equivalent polars
dataframe operations.
Available queries: 1, 2, 3, 5, 6, 28, 33, 44, 54, 56, 60
"""
def get_tpcds_query_nodes(query_num = 1):
    # Query 1----------------------------------------------------------------
    if query_num == 1:
        customer_total_return = ExecutionNode("customer_total_return",
            lambda: read_table("store_returns")
                .join(read_table("date"),
                      left_on = "sr_returned_date_sk",
                      right_on = "d_date_sk")
                .filter(pl.col("d_year") == 2000)
                .groupby(["sr_customer_sk", "sr_store_sk"])
                .agg([pl.sum("sr_pricing_refunded_cash")
                .alias("ctr_total_return")])
                .select(["sr_customer_sk", "sr_store_sk",
                         "ctr_total_return"]),
            [])

        avg_total_return = ExecutionNode("avg_total_return",
            lambda customer_total_return: customer_total_return
                .groupby(["sr_store_sk"])
                .agg([pl.avg("ctr_total_return")
                .alias("ctr_avg_return")])
                .select(["sr_store_sk", "ctr_avg_return"]),
            ["customer_total_return"])

        result = ExecutionNode("result",
            lambda customer_total_return, avg_total_return:
                               customer_total_return
                .join(avg_total_return, on = "sr_store_sk")
                .filter(pl.col("ctr_total_return") >
                        pl.col("ctr_avg_return") * 1.2)
                .join(read_table("store")
                      .filter(pl.col("w_store_address_state") == "TN"),
                      left_on = "sr_store_sk", right_on = "w_store_sk")
                .join(read_table("customer"),
                      left_on = "sr_customer_sk", right_on = "c_customer_sk")
                .select("c_customer_id")
                .sort("c_customer_id")
                .limit(10),
            ["customer_total_return", "avg_total_return"])

        query1_nodes = [customer_total_return, avg_total_return, result]

        return query1_nodes

    # Query 2----------------------------------------------------------------
    if query_num == 2:

        wscs = ExecutionNode("wscs",
            lambda: read_table("web_sales")
                 .select([pl.col("ws_sold_date_sk").alias("sold_date_sk"),
                          pl.col("ws_pricing_ext_sales_price")
                          .alias("sales_price")])
                 .vstack(read_table("catalog_sales")
                         .select([pl.col("cs_sold_date_sk")
                                  .alias("sold_date_sk"),
                                  pl.col("cs_pricing_ext_sales_price")
                                  .alias("sales_price")]))
                 .select([pl.col("sold_date_sk"), pl.col("sales_price")]),
            [])

        wswscs = ExecutionNode("wswscs",
            lambda wscs: wscs
                .join(read_table("date"),
                      left_on = "sold_date_sk", right_on = "d_date_sk")
                .with_columns([
                    pl.when(pl.col("d_dow") == "Sunday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("sun_sales1"),
                    pl.when(pl.col("d_dow") == "Monday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("mon_sales1"),
                    pl.when(pl.col("d_dow") == "Tuesday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("tue_sales1"),
                    pl.when(pl.col("d_dow") == "Wednesday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("wed_sales1"),
                    pl.when(pl.col("d_dow") == "Thursday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("thu_sales1"),
                    pl.when(pl.col("d_dow") == "Friday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("fri_sales1"),
                    pl.when(pl.col("d_dow") == "Saturday")
                             .then(pl.col("sales_price"))
                             .otherwise(0).alias("sat_sales1")])
                .groupby(["d_week_seq"])
                .agg([pl.sum("sun_sales1").alias("sun_sales"),
                      pl.sum("mon_sales1").alias("mon_sales"),
                      pl.sum("tue_sales1").alias("tue_sales"),
                      pl.sum("wed_sales1").alias("wed_sales"),
                      pl.sum("thu_sales1").alias("thu_sales"),
                      pl.sum("fri_sales1").alias("fri_sales"),
                      pl.sum("sat_sales1").alias("sat_sales")])
                .select([pl.col("d_week_seq"), pl.col("sun_sales"),
                         pl.col("mon_sales"), pl.col("tue_sales"),
                         pl.col("wed_sales"), pl.col("thu_sales"),
                         pl.col("fri_sales"), pl.col("sat_sales")]),
            ["wscs"])

        wswscs_2000 = ExecutionNode("wswscs_2000",
            lambda wswscs: wswscs
                .join(read_table("date")
                      .filter(pl.col("d_year") == 2000), on = "d_week_seq")
                .select([pl.col("d_week_seq").alias("d_week_seq1"),
                         pl.col("sun_sales").alias("sun_sales1"),
                         pl.col("mon_sales").alias("mon_sales1"),
                         pl.col("tue_sales").alias("tue_sales1"),
                         pl.col("wed_sales").alias("wed_sales1"),
                         pl.col("thu_sales").alias("thu_sales1"),
                         pl.col("fri_sales").alias("fri_sales1"),
                         pl.col("sat_sales").alias("sat_sales1")]),
            ["wswscs"])

        wswscs_2001 = ExecutionNode("wswscs_2001",
            lambda wswscs: wswscs
                .join(read_table("date")
                      .filter(pl.col("d_year") == 2001), on = "d_week_seq")
                .select([pl.col("d_week_seq").map(lambda x: x - 53)
                         .alias("d_week_seq2"),
                         pl.col("sun_sales").alias("sun_sales2"),
                         pl.col("mon_sales").alias("mon_sales2"),
                         pl.col("tue_sales").alias("tue_sales2"),
                         pl.col("wed_sales").alias("wed_sales2"),
                         pl.col("thu_sales").alias("thu_sales2"),
                         pl.col("fri_sales").alias("fri_sales2"),
                         pl.col("sat_sales").alias("sat_sales2")]),
            ["wswscs"])

        result = ExecutionNode("result",
            lambda wswscs_2000, wswscs_2001: wswscs_2000
                .join(wswscs_2001, left_on = "d_week_seq1",
                      right_on = "d_week_seq2")
                .select([pl.col("d_week_seq1"),
                         pl.col("sun_sales1") / pl.col("sun_sales2"),
                         pl.col("mon_sales1") / pl.col("mon_sales2"),
                         pl.col("tue_sales1") / pl.col("tue_sales2"),
                         pl.col("wed_sales1") / pl.col("wed_sales2"),
                         pl.col("thu_sales1") / pl.col("thu_sales2"),
                         pl.col("fri_sales1") / pl.col("fri_sales2"),
                         pl.col("sat_sales1") / pl.col("sat_sales2")])
                .sort("d_week_seq1"),
            ["wswscs_2000", "wswscs_2001"])
            
        query2_nodes = [wscs, wswscs, wswscs_2000, wswscs_2001, result]

        return query2_nodes

    # Query 3----------------------------------------------------------------
    if query_num == 3:

        result = ExecutionNode("result",
            lambda: read_table("date")
                .filter(pl.col("d_moy") == 11)
                .join(read_table("store_sales"),
                     left_on = "d_date_sk", right_on = "ss_sold_date_sk")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id") == 500),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .groupby(["d_year", "i_brand", "i_brand_id"])
                .agg([pl.sum("ss_pricing_net_profit")
                .alias("ss_pricing_net_profit")])
                .sort(["d_year", "ss_pricing_net_profit", "i_brand_id"],
                      reverse = [False, True, False])
                .limit(10),
            [])
        
        query3_nodes = [result]

        return query3_nodes

    # Query 5----------------------------------------------------------------
    if query_num == 5:

        salesreturns1 = ExecutionNode("salesreturns1",
            lambda: read_table("store_sales")
                .select([pl.col("ss_sold_store_sk").alias("store_sk"),
                         pl.col("ss_sold_date_sk").alias("date_sk"),
                         pl.col("ss_pricing_ext_sales_price")
                         .alias("sales_price"),
                         pl.col("ss_pricing_net_profit").alias("profit"),
                         (pl.col("ss_pricing_ext_sales_price") * 0.0)
                         .alias("return_amt"),
                         (pl.col("ss_pricing_ext_sales_price") * 0.0)
                         .alias("net_loss")])
                .vstack(read_table("store_returns")
                        .select([pl.col("sr_store_sk").alias("store_sk"),
                                 pl.col("sr_returned_date_sk")
                                 .alias("date_sk"),
                                 (pl.col("sr_pricing_reversed_charge") * 0.0)
                                 .alias("sales_price"),
                                 (pl.col("sr_pricing_reversed_charge") * 0.0)
                                 .alias("profit"),
                                 pl.col("sr_pricing_reversed_charge")
                                 .alias("return_amt"),
                                 pl.col("sr_pricing_net_loss")
                                 .alias("net_loss")])),
            [])

        salesreturns2 = ExecutionNode("salesreturns2",
            lambda: read_table("catalog_sales")
                .select([pl.col("cs_catalog_page_sk").alias("page_sk"),
                         pl.col("cs_sold_date_sk").alias("date_sk"),
                         pl.col("cs_pricing_ext_sales_price")
                         .alias("sales_price"),
                         pl.col("cs_pricing_net_profit").alias("profit"),
                         (pl.col("cs_pricing_ext_sales_price") * 0.0)
                         .alias("return_amt"),
                         (pl.col("cs_pricing_ext_sales_price") * 0.0)
                         .alias("net_loss")])
                .vstack(read_table("catalog_returns")
                        .select([pl.col("cr_catalog_page_sk")
                                 .alias("page_sk"),
                                 pl.col("cr_returned_date_sk")
                                 .alias("date_sk"),
                                 (pl.col("cr_pricing_reversed_charge") * 0.0)
                                 .alias("sales_price"),
                                 (pl.col("cr_pricing_reversed_charge") * 0.0)
                                 .alias("profit"),
                                 pl.col("cr_pricing_reversed_charge")
                                 .alias("return_amt"),
                                 pl.col("cr_pricing_net_loss")
                                 .alias("net_loss")])),
            [])

        salesreturns3 = ExecutionNode("salesreturns3",
            lambda: read_table("web_returns")
                .join(read_table("web_sales"),
                      left_on = ["wr_item_sk", "wr_order_number"],
                      right_on = ["ws_item_sk", "ws_order_number"],
                      how = "left")
                .select([pl.col("ws_web_site_sk").alias("wsr_web_site_sk"),
                         pl.col("wr_returned_date_sk").alias("date_sk"),
                         (pl.col("wr_pricing_reversed_charge") * 0.0)
                         .alias("sales_price"),
                         (pl.col("wr_pricing_reversed_charge") * 0.0)
                         .alias("profit"),
                         pl.col("wr_pricing_reversed_charge")
                         .alias("return_amt"),
                         pl.col("wr_pricing_net_loss").alias("net_loss")])
                .vstack(read_table("web_sales")
                        .select([pl.col("ws_web_site_sk")
                                 .alias("wsr_web_site_sk"),
                                 pl.col("ws_sold_date_sk").alias("date_sk"),
                                 pl.col("ws_pricing_ext_sales_price")
                                 .alias("sales_price"),
                                 pl.col("ws_pricing_net_profit")
                                 .alias("profit"),
                                 (pl.col("ws_pricing_ext_sales_price") * 0.0)
                                 .alias("return_amt"),
                                 (pl.col("ws_pricing_ext_sales_price") * 0.0)
                                 .alias("net_loss")])),
            [])
        
        ssr = ExecutionNode("ssr",
            lambda salesreturns1: read_table("date")
                .filter(pl.col("d_month_seq") == 1205)
                .join(salesreturns1, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(read_table("store"),
                      left_on = "store_sk", right_on = "w_store_sk")
                .groupby("w_store_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")])
                .select([pl.col("w_store_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["salesreturns1"])

        csr = ExecutionNode("csr",
            lambda salesreturns2: read_table("date")
                .filter(pl.col("d_month_seq") == 1205)
                .join(salesreturns2, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(read_table("catalog_page"), left_on = "page_sk",
                      right_on = "cp_catalog_page_sk")
                .groupby("cp_catalog_page_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")])
                .select([pl.col("cp_catalog_page_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["salesreturns2"])

        wsr = ExecutionNode("wsr",
            lambda salesreturns3: read_table("date")
                .filter(pl.col("d_month_seq") == 1205)
                .join(salesreturns3, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(read_table("web_site"), left_on = "wsr_web_site_sk",
                      right_on = "web_site_sk")
                .groupby("web_site_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")])
                .select([pl.col("web_site_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["salesreturns3"])

        result = ExecutionNode("result",
            lambda ssr, csr, wsr: ssr.vstack(csr).vstack(wsr)
                .groupby("id")
                .agg([pl.sum("sales").alias("sales"),
                      pl.sum("returns").alias("returns"),
                      pl.sum("profit").alias("profit")])
                .sort("id")
                .limit(10),
            ["ssr", "csr", "wsr"])

        query5_nodes = [salesreturns1, salesreturns2, salesreturns3, ssr,
                        csr, wsr, result]

        return query5_nodes
    
    # Query 6----------------------------------------------------------------
    if query_num == 6:

        date_select = ExecutionNode("date_select",
            lambda: read_table("date")
                .filter((pl.col("d_year") == 2000) & (pl.col("d_moy") == 7)),
            [])

        item_avg = ExecutionNode("item_avg",
            lambda: read_table("item")
                .groupby("i_category")
                .agg(pl.avg("i_current_price")
                .alias("i_current_price_avg")),
            [])

        result = ExecutionNode("result",
            lambda date_select, item_avg: read_table("customer_address")
                .join(read_table("customer"),
                      left_on = "ca_address_sk",
                      right_on = "c_current_addr_sk")
                .join(read_table("store_sales"),
                      left_on = "c_customer_sk",
                      right_on = "ss_sold_customer_sk")
                .join(date_select,
                      left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("item")
                      .join(item_avg, on = "i_category")
                      .filter(pl.col("i_current_price") > 1.2 *
                              pl.col("i_current_price_avg")),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .groupby("i_category")
                .agg(pl.count("i_current_price").alias("count"))
                .filter(pl.col("count") >= 10)
                .limit(10),
            ["date_select", "item_avg"])

        query6_nodes = [date_select, item_avg, result]

        return query6_nodes

    # Query 28----------------------------------------------------------------
    if query_num == 28:
        
        b1 = ExecutionNode("b1",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(0, 5) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b1_lp"),
                         pl.count("ss_pricing_list_price").alias("b1_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b1_cntd")]),
            [])

        b2 = ExecutionNode("b2",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(6, 10) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b2_lp"),
                         pl.count("ss_pricing_list_price").alias("b2_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b2_cntd")]),
            [])

        b3 = ExecutionNode("b3",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(11, 15) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b3_lp"),
                         pl.count("ss_pricing_list_price").alias("b3_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b3_cntd")]),
            [])
        
        b4 = ExecutionNode("b4",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(16, 20) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b4_lp"),
                         pl.count("ss_pricing_list_price").alias("b4_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b4_cntd")]),
            [])

        b5 = ExecutionNode("b5",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(21, 25) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b5_lp"),
                         pl.count("ss_pricing_list_price").alias("b5_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b5_cntd")]),
            [])

        b6 = ExecutionNode("b6",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_pricing_quantity").is_between(26, 30) &
                        (pl.col("ss_pricing_list_price")
                         .is_between(90, 100) |
                         pl.col("ss_pricing_coupon_amt")
                         .is_between(9000, 10000) |
                         pl.col("ss_pricing_wholesale_cost")
                         .is_between(30, 50)))
                .select([pl.avg("ss_pricing_list_price").alias("b6_lp"),
                         pl.count("ss_pricing_list_price").alias("b6_cnt"),
                         pl.n_unique("ss_pricing_list_price")
                         .alias("b6_cntd")]),
            [])

        result = ExecutionNode("result",
            lambda b1, b2, b3, b4, b5, b6: b1.hstack(b2).hstack(b3).hstack(b4)
                .hstack(b5).hstack(b6),
            ["b1", "b2", "b3", "b4", "b5", "b6"])

        query28_nodes = [b1, b2, b3, b4, b5, b6, result]

        return query28_nodes

    # Query 33----------------------------------------------------------------
    if query_num == 33:
        
        category_ss = ExecutionNode("category_ss",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Books", "Home", "Electronics",
                                "Jewelry", "Sports"]))
                .select(pl.col("i_manufact_id")),
            [])

        category_cs = ExecutionNode("category_cs",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Books", "Home", "Electronics",
                                "Jewelry", "Sports"]))
                .select(pl.col("i_manufact_id")),
            [])

        category_ws = ExecutionNode("category_ws",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Books", "Home", "Electronics",
                                "Jewelry", "Sports"]))
                .select(pl.col("i_manufact_id")),
            [])

        ss = ExecutionNode("ss",
            lambda category_ss: read_table("store_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_ss
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ss_sold_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_ss"])

        cs = ExecutionNode("cs",
            lambda category_cs: read_table("catalog_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_cs
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "cs_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "cs_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_cs"])

        ws = ExecutionNode("ws",
            lambda category_ws: read_table("web_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_ws
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ws_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ws_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_ws"])

        tmp1 = ExecutionNode("tmp1",
            lambda ss, cs, ws: ss.vstack(cs).vstack(ws),
            ["ss", "cs", "ws"])

        result = ExecutionNode("result",
            lambda tmp1: tmp1.groupby(pl.col("i_manufact_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp1"])

        query33_nodes = [category_ss, category_cs, category_ws, ss, cs, ws,
                         tmp1, result]

        return query33_nodes

    # Query 44----------------------------------------------------------------
    if query_num == 44:
        
        v1 = ExecutionNode("v1",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_sold_store_sk") == 1 &
                        pl.col("ss_sold_customer_sk").is_null())
                .select(pl.avg("ss_pricing_net_profit").alias("rank_col")),
            [])

        v2 = ExecutionNode("v2",
            lambda: read_table("store_sales")
                .filter(pl.col("ss_sold_store_sk") == 1 &
                        pl.col("ss_sold_customer_sk").is_null())
                .select(pl.avg("ss_pricing_net_profit").alias("rank_col")),
            [])

        v11 = ExecutionNode("v11",
            lambda avg1: read_table("store_sales")
                .filter(pl.col("ss_sold_store_sk") == 1)
                .groupby(pl.col("ss_sold_item_sk"))
                .agg(pl.avg("ss_pricing_net_profit").alias("rank_col"))
                .filter(pl.col("rank_col") > 0.9 * list(avg1
                                     .select(pl.col("rank_col")))[0])
                .select([pl.col("ss_sold_item_sk").alias("item_sk"),
                         pl.col("rank_col")]),
            ["v1"])

        v21 = ExecutionNode("v21",
            lambda avg1: read_table("store_sales")
                .filter(pl.col("ss_sold_store_sk") == 1)
                .groupby(pl.col("ss_sold_item_sk"))
                .agg(pl.avg("ss_pricing_net_profit").alias("rank_col"))
                .filter(pl.col("rank_col") > 0.9 * list(avg1
                                     .select(pl.col("rank_col")))[0])
                .select([pl.col("ss_sold_item_sk").alias("item_sk"),
                         pl.col("rank_col")]),
            ["v2"])

        ascending = ExecutionNode("ascending",
            lambda v1: v1
                .select([pl.col("rank_col").rank(reverse = False).alias("rnk"),
                         pl.col("item_sk")])
                .filter(pl.col("rnk") < 11),
            ["v11"])

        descending = ExecutionNode("descending",
            lambda v2: v2
                .select([pl.col("rank_col").rank(reverse = True).alias("rnk"),
                         pl.col("item_sk")])
                .filter(pl.col("rnk") < 11),
            ["v21"])

        result = ExecutionNode("result",
            lambda ascending, descending: ascending
                .join(read_table("item"),
                      left_on = "item_sk", right_on = "i_item_sk")
                .select([pl.col("rnk"),
                         pl.col("i_product_name").alias("best_performing")])
                .join(descending
                      .join(read_table("item"),
                            left_on = "item_sk", right_on = "i_item_sk")
                      .select([pl.col("rnk"),
                               pl.col("i_product_name")
                               .alias("worst_performing")]), on = "rnk")
                .select([pl.col("rnk"), pl.col("best_performing"),
                         pl.col("worst_performing")])
                .sort("rnk"),
            ["ascending", "descending"])


        query44_nodes = [v1, v2, v11, v21, ascending, descending, result]

        return query44_nodes

    # Query 54----------------------------------------------------------------
    if query_num == 54:
        
        cs_or_ws_sales = ExecutionNode("cs_or_ws_sales",
            lambda: read_table("catalog_sales")
                .select([pl.col("cs_sold_date_sk").alias("sold_date_sk"),
                         pl.col("cs_bill_customer_sk").alias("customer_sk"),
                         pl.col("cs_sold_item_sk").alias("item_sk")])
                .vstack(read_table("web_sales")
                        .select([pl.col("ws_sold_date_sk")
                                 .alias("sold_date_sk"),
                                 pl.col("ws_bill_customer_sk")
                                 .alias("customer_sk"),
                                 pl.col("ws_item_sk")
                                 .alias("item_sk")])),
            [])

        my_customers = ExecutionNode("my_customers",
            lambda cs_or_ws_sales: cs_or_ws_sales
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "sold_date_sk", right_on = "d_date_sk")
                .join(read_table("item"),
                      left_on = "item_sk", right_on = "i_item_sk")
                .join(read_table("customer"),
                      left_on = "customer_sk", right_on = "c_customer_sk")
                .select([pl.col("customer_sk").alias("c_customer_sk"),
                         pl.col("c_current_addr_sk")])
                .drop_duplicates(),
            ["cs_or_ws_sales"])

        seq1 = ExecutionNode("seq1",
            lambda: read_table("date")
                .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7))
                .select(pl.col("d_month_seq").map(lambda x: x + 1))
                .drop_duplicates(),
            [])

        seq2 = ExecutionNode("seq2",
            lambda: read_table("date")
                .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7))
                .select(pl.col("d_month_seq").map(lambda x: x + 3))
                .drop_duplicates(),
            [])

        my_revenue = ExecutionNode("my_revenue",
            lambda my_customers, seq1, seq2: my_customers
                .join(read_table("customer_address"),
                      left_on = "c_current_addr_sk", right_on = "ca_address_sk")
                .join(read_table("store_sales"),
                      left_on = "c_customer_sk",
                      right_on = "ss_sold_customer_sk")
                .join(read_table("date")
                      .filter(pl.col("d_month_seq").is_between(list(seq1
                                     .select(pl.col("d_month_seq")))[0],
                                                            list(seq2
                                     .select(pl.col("d_month_seq")))[0])),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .groupby(pl.col("c_customer_sk"))
                .agg(pl.sum("ss_pricing_ext_sales_price").alias("revenue"))
                .select([pl.col("c_customer_sk"), pl.col("revenue")]),
            ["my_customers", "seq1", "seq2"])

        segments = ExecutionNode("segments",
            lambda my_revenue: my_revenue
                .select(pl.col("revenue").map(lambda x: x / 50)
                        .alias("segment")),
            ["my_revenue"])

        result = ExecutionNode("result",
            lambda segments: segments
                .groupby(pl.col("segment"))
                .agg(pl.count("segment").alias("num_customers"))
                .select([pl.col("segment"),
                         pl.col("num_customers"),
                         pl.col("segment").map(lambda x: x * 50)
                         .alias("segment_base")])
                .sort(["segment", "num_customers"]),
            ["segments"])


        query54_nodes = [cs_or_ws_sales, my_customers, seq1, seq2, my_revenue,
                         segments, result]

        return query54_nodes

    # Query 56----------------------------------------------------------------
    if query_num == 56:
        
        category_ss = ExecutionNode("category_ss",
            lambda: read_table("item")
                .filter(pl.col("i_color")
                        .is_in(["red", "violet", "pink"]))
                .select(pl.col("i_item_id")),
            [])

        category_cs = ExecutionNode("category_cs",
            lambda: read_table("item")
                .filter(pl.col("i_color")
                        .is_in(["red", "violet", "pink"]))
                .select(pl.col("i_item_id")),
            [])

        category_ws = ExecutionNode("category_ws",
            lambda: read_table("item")
                .filter(pl.col("i_color")
                        .is_in(["red", "violet", "pink"]))
                .select(pl.col("i_item_id")),
            [])

        ss = ExecutionNode("ss",
            lambda category_ss: read_table("store_sales")
                .join(read_table("item")
                      .filter(pl.col("i_item_id")
                              .is_in(list(category_ss
                                     .select(pl.col("i_item_id"))))),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ss_sold_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_item_id")
                .agg(pl.sum("ss_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["category_ss"])

        cs = ExecutionNode("cs",
            lambda category_cs: read_table("catalog_sales")
                .join(read_table("item")
                      .filter(pl.col("i_item_id")
                              .is_in(list(category_cs
                                     .select(pl.col("i_item_id"))))),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "cs_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "cs_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_item_id")
                .agg(pl.sum("cs_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["category_cs"])

        ws = ExecutionNode("ws",
            lambda category_ws: read_table("web_sales")
                .join(read_table("item")
                      .filter(pl.col("i_item_id")
                              .is_in(list(category_ws
                                     .select(pl.col("i_item_id"))))),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ws_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ws_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_item_id")
                .agg(pl.sum("ws_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["category_ws"])

        tmp1 = ExecutionNode("tmp1",
            lambda ss, cs, ws: ss.vstack(cs).vstack(ws),
            ["ss", "cs", "ws"])

        result = ExecutionNode("result",
            lambda tmp1: tmp1.groupby(pl.col("i_item_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_item_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp1"])

        query56_nodes = [category_ss, category_cs, category_ws, ss, cs, ws,
                         tmp1, result]

        return query56_nodes

    # Query 60----------------------------------------------------------------
    if query_num == 60:
        
        category_ss = ExecutionNode("category_ss",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Children", "Men", "Music",
                                "Jewelry", "Shoes"]))
                .select(pl.col("i_manufact_id")),
            [])

        category_cs = ExecutionNode("category_cs",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Children", "Men", "Music",
                                "Jewelry", "Shoes"]))
                .select(pl.col("i_manufact_id")),
            [])

        category_ws = ExecutionNode("category_ws",
            lambda: read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Children", "Men", "Music",
                                "Jewelry", "Shoes"]))
                .select(pl.col("i_manufact_id")),
            [])

        ss = ExecutionNode("ss",
            lambda category_ss: read_table("store_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_ss
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ss_sold_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_ss"])

        cs = ExecutionNode("cs",
            lambda category_cs: read_table("catalog_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_cs
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "cs_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "cs_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_cs"])

        ws = ExecutionNode("ws",
            lambda category_ws: read_table("web_sales")
                .join(read_table("item")
                      .filter(pl.col("i_manufact_id")
                              .is_in(list(category_ws
                                     .select(pl.col("i_manufact_id"))))),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(read_table("date")
                      .filter((pl.col("d_year") == 2000) &
                              (pl.col("d_moy") == 7)),
                      left_on = "ws_sold_date_sk",
                      right_on = "d_date_sk")
                .join(read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -6),
                      left_on = "ws_bill_addr_sk",
                      right_on = "ca_address_sk")
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price").alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["category_ws"])

        tmp1 = ExecutionNode("tmp1",
            lambda ss, cs, ws: ss.vstack(cs).vstack(ws),
            ["ss", "cs", "ws"])

        result = ExecutionNode("result",
            lambda tmp1: tmp1.groupby(pl.col("i_manufact_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp1"])

        query60_nodes = [category_ss, category_cs, category_ws, ss, cs, ws,
                         tmp1, result]

        return query60_nodes

    # Query 5 (Old)----------------------------------------------------------------
    if query_num == 999:
        store_sales = ExecutionNode("store_sales",
                                    lambda: read_table("store_sales"), [])
        store_returns = ExecutionNode("store_returns",
                                      lambda: read_table("store_returns"), [])
        catalog_sales = ExecutionNode("catalog_sales",
                                      lambda: read_table("catalog_sales"), [])
        catalog_returns = ExecutionNode("catalog_returns",
                                    lambda: read_table("catalog_returns"), [])
        web_sales = ExecutionNode("web_sales",
                                  lambda: read_table("web_sales"), [])
        web_returns = ExecutionNode("web_returns",
                                    lambda: read_table("web_returns"), [])

        date = ExecutionNode("date", lambda: read_table("date"), [])
        store = ExecutionNode("store", lambda: read_table("store"), [])
        catalog_page = ExecutionNode("catalog_page",
                                     lambda: read_table("catalog_page"), [])
        web_site = ExecutionNode("web_site", lambda: read_table("web_site"), [])

        sr_select = ExecutionNode("sr_select",
            lambda sr: sr.select([pl.col("sr_store_sk").alias("store_sk"),
                pl.col("sr_returned_date_sk").alias("date_sk"),
                (pl.col("sr_pricing_reversed_charge") * 0.0)
                                  .alias("sales_price"),
                (pl.col("sr_pricing_reversed_charge") * 0.0).alias("profit"),
                pl.col("sr_pricing_reversed_charge").alias("return_amt"),
                pl.col("sr_pricing_net_loss").alias("net_loss")]),
            ["store_returns"])

        ss_select = ExecutionNode("ss_select",
            lambda ss: ss.select([pl.col("ss_sold_store_sk").alias("store_sk"),
                pl.col("ss_sold_date_sk").alias("date_sk"),
                pl.col("ss_pricing_ext_sales_price").alias("sales_price"),
                pl.col("ss_pricing_net_profit").alias("profit"),
                (pl.col("ss_pricing_ext_sales_price") * 0.0)
                                  .alias("return_amt"),
                (pl.col("ss_pricing_ext_sales_price") * 0.0)
                                  .alias("net_loss")]),
            ["store_sales"])

        cr_select = ExecutionNode("cr_select",
            lambda sr: sr.select([pl.col("cr_catalog_page_sk").alias("page_sk"),
                pl.col("cr_returned_date_sk").alias("date_sk"),
                (pl.col("cr_pricing_reversed_charge") * 0.0)
                                  .alias("sales_price"),
                (pl.col("cr_pricing_reversed_charge") * 0.0).alias("profit"),
                pl.col("cr_pricing_reversed_charge").alias("return_amt"),
                pl.col("cr_pricing_net_loss").alias("net_loss")]),
            ["catalog_returns"])

        cs_select = ExecutionNode("cs_select",
            lambda cs: cs.select([pl.col("cs_catalog_page_sk").alias("page_sk"),
                pl.col("cs_sold_date_sk").alias("date_sk"),
                pl.col("cs_pricing_ext_sales_price").alias("sales_price"),
                pl.col("cs_pricing_net_profit").alias("profit"),
                (pl.col("cs_pricing_ext_sales_price") * 0.0)
                                  .alias("return_amt"),
                (pl.col("cs_pricing_ext_sales_price") * 0.0)
                                  .alias("net_loss")]),
            ["catalog_sales"])

        ws_select = ExecutionNode("ws_select",
            lambda cs: cs.select([pl.col("ws_web_site_sk")
                                  .alias("wsr_web_site_sk"),
                pl.col("ws_sold_date_sk").alias("date_sk"),
                pl.col("ws_pricing_ext_sales_price").alias("sales_price"),
                pl.col("ws_pricing_net_profit").alias("profit"),
                (pl.col("ws_pricing_ext_sales_price") * 0.0)
                                  .alias("return_amt"),
                (pl.col("ws_pricing_ext_sales_price") * 0.0)
                                  .alias("net_loss")]),
            ["web_sales"])

        wsr2 = ExecutionNode("wsr2",
            lambda web_returns, web_sales: web_returns
                .join(web_sales, left_on = ["wr_item_sk", "wr_order_number"],
                      right_on = ["ws_item_sk", "ws_order_number"],
                      how = "left"),
            ["web_returns", "web_sales"])

        wsr2_select = ExecutionNode("wsr2_select",
            lambda sr: sr.select([pl.col("ws_web_site_sk")
                                  .alias("wsr_web_site_sk"),
                pl.col("wr_returned_date_sk").alias("date_sk"),
                (pl.col("wr_pricing_reversed_charge") * 0.0)
                                  .alias("sales_price"),
                (pl.col("wr_pricing_reversed_charge") * 0.0).alias("profit"),
                pl.col("wr_pricing_reversed_charge").alias("return_amt"),
                pl.col("wr_pricing_net_loss").alias("net_loss")]),
            ["wsr2"])

        salesreturns1 = ExecutionNode("salesreturns1",
            lambda ss_select, sr_select: ss_select.vstack(sr_select),
            ["ss_select", "sr_select"])

        salesreturns2 = ExecutionNode("salesreturns2",
            lambda cs_select, cr_select: cs_select.vstack(cr_select),
            ["cs_select", "cr_select"])

        salesreturns3 = ExecutionNode("salesreturns3",
            lambda ws_select, wsr_select: ws_select.vstack(wsr_select),
            ["ws_select", "wsr2_select"])

        date_select = ExecutionNode("date_select",
            lambda date: date.filter(pl.col("d_month_seq") == 1205),
            ["date"])

        date_sr1_join = ExecutionNode("date_sr1_join",
            lambda date_select, salesreturns1: date_select
                .join(salesreturns1, left_on = "d_date_sk",
                      right_on = "date_sk"),
            ["date_select", "salesreturns1"])

        date_sr2_join = ExecutionNode("date_sr2_join",
            lambda date_select, salesreturns2: date_select
                .join(salesreturns2, left_on = "d_date_sk",
                      right_on = "date_sk"),
            ["date_select", "salesreturns2"])

        date_sr3_join = ExecutionNode("date_sr3_join",
            lambda date_select, salesreturns3: date_select
                .join(salesreturns3, left_on = "d_date_sk",
                      right_on = "date_sk"),
            ["date_select", "salesreturns3"])

        ssr = ExecutionNode("ssr",
            lambda date_sr1_join, store: date_sr1_join
                .join(store, left_on = "store_sk", right_on = "w_store_sk"),
            ["date_sr1_join", "store"])

        csr = ExecutionNode("csr",
            lambda date_sr2_join, catalog_page: date_sr2_join
                .join(catalog_page, left_on = "page_sk",
                      right_on = "cp_catalog_page_sk"),
            ["date_sr2_join", "catalog_page"])

        wsr = ExecutionNode("wsr",
            lambda date_sr3_join, web_site: date_sr3_join
                .join(web_site, left_on = "wsr_web_site_sk",
                      right_on = "web_site_sk"),
            ["date_sr3_join", "web_site"])

        ssr_groupby = ExecutionNode("ssr_groupby",
            lambda ssr: ssr.groupby("w_store_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")]),
            ["ssr"])

        csr_groupby = ExecutionNode("csr_groupby",
            lambda csr: csr.groupby("cp_catalog_page_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")]),
            ["csr"])

        wsr_groupby = ExecutionNode("wsr_groupby",
            lambda wsr: wsr.groupby("web_site_id")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit"),
                      pl.sum("return_amt").alias("returns"),
                      pl.sum("net_loss").alias("profit_loss")]),
            ["wsr"])

        ssr_select = ExecutionNode("ssr_select",
            lambda ssr_groupby: ssr_groupby
                .select([pl.col("w_store_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["ssr_groupby"])

        csr_select = ExecutionNode("csr_select",
            lambda csr_groupby: csr_groupby
                .select([pl.col("cp_catalog_page_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["csr_groupby"])

        wsr_select = ExecutionNode("wsr_select",
            lambda wsr_groupby: wsr_groupby
                .select([pl.col("web_site_id").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["wsr_groupby"])

        result_union = ExecutionNode("result_union",
            lambda ssr_select, csr_select, wsr_select: ssr_select
                                     .vstack(csr_select).vstack(wsr_select),
            ["ssr_select", "csr_select", "wsr_select"])

        result_groupby = ExecutionNode("result_groupby",
            lambda result_union: result_union.groupby("id")
                .agg([pl.sum("sales").alias("sales"),
                      pl.sum("returns").alias("returns"),
                      pl.sum("profit").alias("profit")]),
            ["result_union"])

        result_order = ExecutionNode("result_order",
            lambda result_groupby: result_groupby.sort("id"),
            ["result_groupby"])

        result = ExecutionNode("result",
            lambda result_order: result_order.limit(10),
            ["result_order"])

        query5_nodes = [store_sales, store_returns, catalog_sales,
                        catalog_returns, web_sales, web_returns, sr_select,
                        ss_select, cr_select, cs_select, wsr2, ws_select,
                        wsr2_select, salesreturns1, salesreturns2,
                        salesreturns3, date, date_select, date_sr1_join,
                        date_sr2_join, date_sr3_join, store, catalog_page,
                        web_site, ssr, csr, wsr, ssr_groupby, csr_groupby,
                        wsr_groupby, ssr_select, csr_select, wsr_select,
                        result_union, result_groupby, result_order, result]

        return query5_nodes
