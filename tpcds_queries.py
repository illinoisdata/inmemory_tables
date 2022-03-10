from ExecutionNode import *
from TableReader import *
import polars as pl
import numpy as np

"""
A file containing select TPC-DS queries translated into equivalent polars
dataframe operations.
Available queries: 1, 2, 3, 5, 6, 28, 33, 44, 54, 56, 60
"""
def get_tpcds_query_nodes(job_num = 1):
    tablereader = TableReader()
    
    # Job 1 (Queries 5, 77, 80)--------------------------------------------------------
    if job_num == 1:

        # Query 5
        ss = ExecutionNode("ss",
            lambda: tablereader.read_table("store_sales")
                .select([pl.col("ss_sold_store_sk").alias("store_sk"),
                         pl.col("ss_sold_date_sk").alias("date_sk"),
                         pl.col("ss_pricing_ext_sales_price")
                         .alias("sales_price"),
                         pl.col("ss_pricing_net_profit").alias("profit"),
                         (pl.col("ss_pricing_ext_sales_price") * 0.0)
                         .alias("return_amt"),
                         (pl.col("ss_pricing_ext_sales_price") * 0.0)
                         .alias("net_loss"),
                         pl.col("ss_sold_item_sk"),
                         pl.col("ss_ticket_number"),
                         pl.col("ss_sold_store_sk"),
                         pl.col("ss_sold_date_sk"),
                         pl.col("ss_sold_promo_sk")]),
            [], tablereader)
        
        sr = ExecutionNode("sr",
            lambda: tablereader.read_table("store_returns")
                .select([pl.col("sr_store_sk").alias("store_sk"),
                         pl.col("sr_returned_date_sk").alias("date_sk"),
                         (pl.col("sr_pricing_reversed_charge") * 0.0)
                         .alias("sales_price"),
                         (pl.col("sr_pricing_reversed_charge") * 0.0)
                         .alias("profit"),
                         pl.col("sr_pricing_reversed_charge")
                         .alias("return_amt"),
                         pl.col("sr_pricing_net_loss").alias("net_loss"),
                         pl.col("sr_item_sk"), pl.col("sr_ticket_number")]),
            [], tablereader)
                            
        salesreturns1 = ExecutionNode("salesreturns1",
            lambda ss, sr: ss
                .drop(["ss_sold_item_sk", "ss_ticket_number",
                       "ss_sold_store_sk", "ss_sold_date_sk",
                       "ss_sold_promo_sk"])
                .vstack(sr.drop(["sr_item_sk", "sr_ticket_number"])),
            ["ss", "sr"], tablereader)

        cs = ExecutionNode("cs",
            lambda: tablereader.read_table("catalog_sales")
                .select([pl.col("cs_catalog_page_sk").alias("page_sk"),
                         pl.col("cs_sold_date_sk").alias("date_sk"),
                         pl.col("cs_pricing_ext_sales_price")
                         .alias("sales_price"),
                         pl.col("cs_pricing_net_profit").alias("profit"),
                         (pl.col("cs_pricing_ext_sales_price") * 0.0)
                         .alias("return_amt"),
                         (pl.col("cs_pricing_ext_sales_price") * 0.0)
                         .alias("net_loss"),
                         pl.col("cs_sold_item_sk"),
                         pl.col("cs_order_number"),
                         pl.col("cs_catalog_page_sk"),
                         pl.col("cs_sold_date_sk"),
                         pl.col("cs_promo_sk")]),
            [], tablereader)

        cr = ExecutionNode("cr",
            lambda: tablereader.read_table("catalog_returns")
                .select([pl.col("cr_catalog_page_sk").alias("page_sk"),
                         pl.col("cr_returned_date_sk").alias("date_sk"),
                         (pl.col("cr_pricing_reversed_charge") * 0.0)
                         .alias("sales_price"),
                         (pl.col("cr_pricing_reversed_charge") * 0.0)
                         .alias("profit"),
                         pl.col("cr_pricing_reversed_charge")
                         .alias("return_amt"),
                         pl.col("cr_pricing_net_loss").alias("net_loss"),
                         pl.col("cr_item_sk"), pl.col("cr_order_number")]),
            [], tablereader)

        salesreturns2 = ExecutionNode("salesreturns2",
            lambda cs, cr: cs
                .drop(["cs_sold_item_sk", "cs_order_number",
                       "cs_catalog_page_sk", "cs_sold_date_sk",
                       "cs_promo_sk"])
                .vstack(cr.drop(["cr_item_sk", "cr_order_number"])),
            ["cs", "cr"], tablereader)

        ws = ExecutionNode("ws",
            lambda: tablereader.read_table("web_sales")
                .select([pl.col("ws_web_site_sk").alias("wsr_web_site_sk"),
                         pl.col("ws_sold_date_sk").alias("date_sk"),
                         pl.col("ws_pricing_ext_sales_price")
                         .alias("sales_price"),
                         pl.col("ws_pricing_net_profit").alias("profit"),
                         (pl.col("ws_pricing_ext_sales_price") * 0.0)
                         .alias("return_amt"),
                         (pl.col("ws_pricing_ext_sales_price") * 0.0)
                         .alias("net_loss"),
                         pl.col("ws_web_page_sk"), pl.col("ws_promo_sk"),
                         pl.col("ws_item_sk"), pl.col("ws_order_number")]),
            [], tablereader)

        wr = ExecutionNode("wr",
            lambda ws: tablereader.read_table("web_returns")
                .join(ws,
                      left_on = ["wr_item_sk", "wr_order_number"],
                      right_on = ["ws_item_sk", "ws_order_number"],
                      how = "left")
                .select([pl.col("wsr_web_site_sk"),
                         pl.col("wr_returned_date_sk").alias("date_sk"),
                         (pl.col("wr_pricing_reversed_charge") * 0.0)
                         .alias("sales_price"),
                         (pl.col("wr_pricing_reversed_charge") * 0.0)
                         .alias("profit"),
                         pl.col("wr_pricing_reversed_charge")
                         .alias("return_amt"),
                         pl.col("wr_pricing_net_loss").alias("net_loss"),
                         pl.col("wr_item_sk"), pl.col("wr_order_number")]),
            ["ws"], tablereader)

        salesreturns3 = ExecutionNode("salesreturns3",
            lambda ws, wr: ws
                .drop(["ws_item_sk", "ws_web_page_sk",
                       "ws_order_number", "ws_promo_sk"])
                .vstack(wr.drop(["wr_item_sk", "wr_order_number"])),
            ["ws", "wr"], tablereader)
        
        ssr = ExecutionNode("ssr",
            lambda salesreturns1: tablereader.read_table("date")
                .filter(pl.col("d_month_seq") == 1207)
                .join(salesreturns1, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(tablereader.read_table("store"),
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
            ["salesreturns1"], tablereader)

        csr = ExecutionNode("csr",
            lambda salesreturns2: tablereader.read_table("date")
                .filter(pl.col("d_month_seq") == 1207)
                .join(salesreturns2, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(tablereader.read_table("catalog_page"),
                      left_on = "page_sk",
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
            ["salesreturns2"], tablereader)

        wsr = ExecutionNode("wsr",
            lambda salesreturns3: tablereader.read_table("date")
                .filter(pl.col("d_month_seq") == 1207)
                .join(salesreturns3, left_on = "d_date_sk",
                      right_on = "date_sk")
                .join(tablereader.read_table("web_site"),
                      left_on = "wsr_web_site_sk",
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
            ["salesreturns3"], tablereader)

        scw_stack = ExecutionNode("scw_stack",
            lambda ssr, csr, wsr: ssr.vstack(csr).vstack(wsr).distinct(),
            ["ssr", "csr", "wsr"], tablereader)

        query5_result = ExecutionNode("query5_result",
            lambda scw_stack: scw_stack
                .groupby("id")
                .agg([pl.sum("sales").alias("sales"),
                      pl.sum("returns").alias("returns"),
                      pl.sum("profit").alias("profit")])
                .sort("id")
                .limit(100),
            ["scw_stack"], tablereader)

        query5_nodes = [ss, sr, cs, cr, ws, wr, salesreturns1, salesreturns2,
                        salesreturns3, ssr, csr, wsr, scw_stack, query5_result]


        # Query 77
        ss_groupby = ExecutionNode("ss_groupby",
            lambda ss: ss.groupby("store_sk")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit_gain")])
                .select([pl.col("store_sk"), pl.col("sales"),
                         pl.col("profit_gain")]),
            ["ss"], tablereader)

        sr_groupby = ExecutionNode("sr_groupby",
            lambda sr: sr.groupby("store_sk")
                .agg([pl.sum("return_amt").alias("returns"),
                      pl.sum("profit").alias("profit_loss")])
                .select([pl.col("store_sk"), pl.col("returns"),
                         pl.col("profit_loss")]),
            ["sr"], tablereader)

        cs_groupby = ExecutionNode("cs_groupby",
            lambda cs: cs.groupby("page_sk")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit_gain")])
                .select([pl.col("page_sk"), pl.col("sales"),
                         pl.col("profit_gain")]),
            ["cs"], tablereader)

        cr_groupby = ExecutionNode("cr_groupby",
            lambda cr: cr.groupby("page_sk")
                .agg([pl.sum("return_amt").alias("returns"),
                      pl.sum("profit").alias("profit_loss")])
                .select([pl.col("page_sk"), pl.col("returns"),
                         pl.col("profit_loss")]),
            ["cr"], tablereader)

        ws_groupby = ExecutionNode("ws_groupby",
            lambda ws: ws
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_month_seq") == 1207),
                      left_on = "date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("web_page"),
                      left_on = "ws_web_page_sk", right_on = "wp_page_sk")
                .groupby("ws_web_page_sk")
                .agg([pl.sum("sales_price").alias("sales"),
                      pl.sum("profit").alias("profit_gain")])
                .select([pl.col("ws_web_page_sk"), pl.col("sales"),
                         pl.col("profit_gain")]),
            ["ws"], tablereader)

        wr_groupby = ExecutionNode("wr_groupby",
            lambda: tablereader.read_table("web_returns")
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_month_seq") == 1207),
                      left_on = "wr_returned_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("web_page"),
                      left_on = "wr_web_page_sk", right_on = "wp_page_sk")
                .groupby("wr_web_page_sk")
                .agg([pl.sum("wr_pricing_reversed_charge").alias("returns"),
                      pl.sum("wr_pricing_net_loss").alias("profit_loss")])
                .select([pl.col("wr_web_page_sk"), pl.col("returns"),
                         pl.col("profit_loss")]),
            [], tablereader)

        ss_sr_join = ExecutionNode("ss_sr_join",
            lambda ss_groupby, sr_groupby: ss_groupby
                .join(sr_groupby, on = "store_sk", how = "left")
                .fill_null(0)
                .select([pl.lit("store channel").alias("channel"),
                         pl.col("store_sk").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit_gain") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["ss_groupby", "sr_groupby"], tablereader)

        cs_cr_join = ExecutionNode("cs_cr_join",
            lambda cs_groupby, cr_groupby: cs_groupby
                .join(cr_groupby, on = "page_sk", how = "left")
                .fill_null(0)
                .select([pl.lit("catalog channel").alias("channel"),
                         pl.col("page_sk").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit_gain") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["cs_groupby", "cr_groupby"], tablereader)

        ws_wr_join = ExecutionNode("ws_wr_join",
            lambda ws_groupby, wr_groupby: ws_groupby
                .join(wr_groupby,
                      left_on = "ws_web_page_sk",
                      right_on = "wr_web_page_sk", how = "left")
                .fill_null(0)
                .select([pl.lit("web channel").alias("channel"),
                         pl.col("ws_web_page_sk").alias("id"),
                         pl.col("sales"), pl.col("returns"),
                         (pl.col("profit_gain") - pl.col("profit_loss"))
                         .alias("profit")]),
            ["ws_groupby", "wr_groupby"], tablereader)

        x = ExecutionNode("x",
            lambda ss_sr_join, cs_cr_join, ws_wr_join: ss_sr_join
                .vstack(cs_cr_join).vstack(ws_wr_join).distinct(),
            ["ss_sr_join", "cs_cr_join", "ws_wr_join"], tablereader)

        query77_result = ExecutionNode("query77_result",
            lambda x: x.groupby(["channel", "id"])
                .agg([pl.sum("sales").alias("sum_sales"),
                      pl.sum("returns").alias("sum_returns"),
                      pl.sum("profit").alias("sum_profit")])
                .select([pl.col("channel"), pl.col("id"), pl.col("sum_sales"),
                         pl.col("sum_returns"), pl.col("sum_profit")])
                .sort(["channel", "id"])
                .limit(100),
            ["x"], tablereader)

        query77_nodes = [ss_groupby, sr_groupby, ws_groupby, wr_groupby,
                         cs_groupby, cr_groupby, ss_sr_join, cs_cr_join,
                         ws_wr_join, x, query77_result]

        # Query 80
        ssr2 = ExecutionNode("ssr2",
            lambda ss, sr: ss
                .join(sr,
                      left_on = ["ss_sold_item_sk", "ss_ticket_number"],
                      right_on = ["sr_item_sk", "sr_ticket_number"],
                      how = "left")
                .fill_null(0)
                .join(tablereader.read_table("store"),
                      left_on = "ss_sold_store_sk", right_on = "w_store_sk")
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_month_seq") == 1207),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .filter(pl.col("i_current_price") > 50),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("promotion")
                      .filter(pl.col("p_channel_tv") == "N"),
                      left_on = "ss_sold_promo_sk", right_on = "p_promo_sk")
                .groupby("w_store_id")
                .agg([pl.sum("sales_price").alias("sales2"),
                      pl.sum("return_amt").alias("returns2"),
                      pl.sum("profit").alias("profit2"),
                      pl.sum("net_loss").alias("net_loss2")])
                .select([pl.lit("store channel").alias("channel"),
                         pl.col("w_store_id").alias("id"),
                         pl.col("sales2"), pl.col("returns2"),
                         (pl.col("profit2") - pl.col("net_loss2"))
                         .alias("net_profit")]),
            ["ss", "sr"], tablereader)

        csr2 = ExecutionNode("csr2",
            lambda cs, cr: cs
                .join(cr,
                      left_on = ["cs_sold_item_sk", "cs_order_number"],
                      right_on = ["cr_item_sk", "cr_order_number"],
                      how = "left")
                .fill_null(0)
                .join(tablereader.read_table("catalog_page"),
                      left_on = "cs_catalog_page_sk",
                      right_on = "cp_catalog_page_sk")
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_month_seq") == 1207),
                      left_on = "cs_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .filter(pl.col("i_current_price") > 50),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("promotion")
                      .filter(pl.col("p_channel_tv") == "N"),
                      left_on = "cs_promo_sk", right_on = "p_promo_sk")
                .groupby("cp_catalog_page_id")
                .agg([pl.sum("sales_price").alias("sales2"),
                      pl.sum("return_amt").alias("returns2"),
                      pl.sum("profit").alias("profit2"),
                      pl.sum("net_loss").alias("net_loss2")])
                .select([pl.lit("catalog channel").alias("channel"),
                         pl.col("cp_catalog_page_id").alias("id"),
                         pl.col("sales2"), pl.col("returns2"),
                         (pl.col("profit2") - pl.col("net_loss2"))
                         .alias("net_profit")]),
            ["cs", "cr"], tablereader)

        wsr2 = ExecutionNode("wsr2",
            lambda ws: ws
                .join(tablereader.read_table("web_returns"),
                      left_on = ["ws_item_sk", "ws_order_number"],
                      right_on = ["wr_item_sk", "wr_order_number"],
                      how = "left")
                .fill_null(0)
                .join(tablereader.read_table("web_site"),
                      left_on = "wsr_web_site_sk",
                      right_on = "web_site_sk")
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_month_seq") == 1207),
                      left_on = "date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .filter(pl.col("i_current_price") > 50),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("promotion")
                      .filter(pl.col("p_channel_tv") == "N"),
                      left_on = "ws_promo_sk", right_on = "p_promo_sk")
                .groupby("web_site_id")
                .agg([pl.sum("sales_price").alias("sales2"),
                      pl.sum("wr_pricing_reversed_charge").alias("returns2"),
                      pl.sum("profit").alias("profit2"),
                      pl.sum("wr_pricing_net_loss").alias("net_loss2")])
                .select([pl.lit("web channel").alias("channel"),
                         pl.col("web_site_id").alias("id"),
                         pl.col("sales2"), pl.col("returns2"),
                         (pl.col("profit2") - pl.col("net_loss2"))
                         .alias("net_profit")]),
            ["ws"], tablereader)

        x2 = ExecutionNode("x2",
            lambda ssr2, csr2, wsr2: ssr2.vstack(csr2).vstack(wsr2).distinct(),
            ["ssr2", "csr2", "wsr2"], tablereader)

        query80_result = ExecutionNode("query80_result",
            lambda x2: x2.groupby(["channel", "id"])
                .agg([pl.sum("sales2").alias("sum_sales"),
                      pl.sum("returns2").alias("sum_returns"),
                      pl.sum("net_profit").alias("sum_profit")])
                .select([pl.col("channel"), pl.col("id"), pl.col("sum_sales"),
                         pl.col("sum_returns"), pl.col("sum_profit")])
                .sort(["channel", "id"])
                .limit(100),
            ["x2"], tablereader)

        query80_nodes = [ssr2, csr2, wsr2, x2, query80_result]

        job1_nodes = query5_nodes + query77_nodes + query80_nodes
                           
        return job1_nodes, tablereader

    # Job 2 (Queries 33, 56, 60, 61)--------------------------------------------------------
    if job_num == 2:

        category_q33 = ExecutionNode("category_q33",
            lambda: tablereader.read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Books", "Home", "Electronics",
                                "Jewelry", "Sports"]))
                .select(pl.col("i_manufact_id")),
            [], tablereader)

        category_q56 = ExecutionNode("category_q56",
            lambda: tablereader.read_table("item")
                .filter(pl.col("i_color")
                        .is_in(["slate", "blanched", "burnished"]))
                .select(pl.col("i_item_id")),
            [], tablereader)

        category_q60 = ExecutionNode("category_q60",
            lambda: tablereader.read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Children", "Men", "Music",
                                "Jewelry", "Shoes"]))
                .select(pl.col("i_manufact_id")),
            [], tablereader)

        category_q61 = ExecutionNode("category_q61",
            lambda: tablereader.read_table("item")
                .filter(pl.col("i_category")
                        .is_in(["Books", "Home", "Electronics",
                                "Jewelry", "Sports"]))
                .select(pl.col("i_manufact_id")),
            [], tablereader)

        ss = ExecutionNode("ss",
            lambda: tablereader.read_table("store_sales")
                .join(tablereader.read_table("item"),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2001) &
                              (pl.col("d_moy") == 2)),
                      left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk")
                .join(tablereader.read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -5),
                      left_on = "ss_sold_addr_sk",
                      right_on = "ca_address_sk"),
            [], tablereader)

        cs = ExecutionNode("cs",
            lambda: tablereader.read_table("catalog_sales")
                .join(tablereader.read_table("item"),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2001) &
                              (pl.col("d_moy") == 2)),
                      left_on = "cs_sold_date_sk",
                      right_on = "d_date_sk")
                .join(tablereader.read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -5),
                      left_on = "cs_bill_addr_sk",
                      right_on = "ca_address_sk"),
            [], tablereader)

        ws = ExecutionNode("ws",
            lambda: tablereader.read_table("web_sales")
                .join(tablereader.read_table("item"),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2001) &
                              (pl.col("d_moy") == 2)),
                      left_on = "ws_sold_date_sk",
                      right_on = "d_date_sk")
                .join(tablereader.read_table("customer_address")
                      .filter(pl.col("ca_address_gmt_offset") == -5),
                      left_on = "ws_bill_addr_sk",
                      right_on = "ca_address_sk"),
            [], tablereader)

        ss_q33 = ExecutionNode("ss_q33",
            lambda ss, category_q33: ss
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q33["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ss", "category_q33"], tablereader)

        cs_q33 = ExecutionNode("cs_q33",
            lambda cs, category_q33: cs
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q33["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["cs", "category_q33"], tablereader)

        ws_q33 = ExecutionNode("ws_q33",
            lambda ws, category_q33: ws
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q33["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ws", "category_q33"], tablereader)
            
        ss_q56 = ExecutionNode("ss_q56",
            lambda ss, category_q56: ss
                .filter(pl.col("i_item_id")
                        .is_in(category_q56["i_item_id"].to_list()))
                .groupby("i_item_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["ss", "category_q56"], tablereader)

        cs_q56 = ExecutionNode("cs_q56",
            lambda cs, category_q56: cs
                .filter(pl.col("i_item_id")
                        .is_in(category_q56["i_item_id"].to_list()))
                .groupby("i_item_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["cs", "category_q56"], tablereader)

        ws_q56 = ExecutionNode("ws_q56",
            lambda ws, category_q56: ws
                .filter(pl.col("i_item_id")
                        .is_in(category_q56["i_item_id"].to_list()))
                .groupby("i_item_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["ws", "category_q56"], tablereader)

        ss_q60 = ExecutionNode("ss_q60",
            lambda ss, category_q60: ss
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q60["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ss", "category_q60"], tablereader)

        cs_q60 = ExecutionNode("cs_q60",
            lambda cs, category_q60: cs
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q60["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["cs", "category_q60"], tablereader)

        ws_q60 = ExecutionNode("ws_q60",
            lambda ws, category_q60: ws
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q60["i_manufact_id"].to_list()))
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ws", "category_q60"], tablereader)


        tmp_q33 = ExecutionNode("tmp_q33",
            lambda ss_q33, cs_q33, ws_q33: ss_q33.vstack(cs_q33)
                .vstack(ws_q33).distinct(),
            ["ss_q33", "cs_q33", "ws_q33"], tablereader)

        tmp_q56 = ExecutionNode("tmp_q56",
            lambda ss_q56, cs_q56, ws_q56: ss_q56.vstack(cs_q56)
                .vstack(ws_q56).distinct(),
            ["ss_q56", "cs_q56", "ws_q56"], tablereader)

        tmp_q60 = ExecutionNode("tmp_q60",
            lambda ss_q60, cs_q60, ws_q60: ss_q60.vstack(cs_q60)
                .vstack(ws_q60).distinct(),
            ["ss_q60", "cs_q60", "ws_q60"], tablereader)

        query33_result = ExecutionNode("query33_result",
            lambda tmp_q33: tmp_q33.groupby(pl.col("i_manufact_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp_q33"], tablereader)

        query56_result = ExecutionNode("query56_result",
            lambda tmp_q56: tmp_q56.groupby(pl.col("i_item_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_item_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp_q56"], tablereader)

        query60_result = ExecutionNode("query60_result",
            lambda tmp_q60: tmp_q60.groupby(pl.col("i_manufact_id"))
                .agg(pl.sum("total_sales").alias("total_sales_sum"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales_sum")])
                .sort("total_sales_sum"),
            ["tmp_q60"], tablereader)

        ss_q61 = ExecutionNode("ss_q61",
            lambda ss, category_q61: ss
                .filter(pl.col("i_manufact_id")
                        .is_in(category_q61["i_manufact_id"].to_list()))
                .join(tablereader.read_table("store")
                      .filter(pl.col("w_store_address_gmt_offset") == -5),
                      left_on = "ss_sold_store_sk",
                      right_on = "w_store_sk")
                .join(tablereader.read_table("customer"),
                      left_on = "ss_sold_customer_sk",
                      right_on = "c_customer_sk"),
            ["ss", "category_q61"], tablereader)

        promotions = ExecutionNode("promotions",
            lambda ss_q61: ss_q61
                .join(tablereader.read_table("promotion")
                      .filter((pl.col("p_channel_dmail") == "Y") |
                              (pl.col("p_channel_email") == "Y") |
                              (pl.col("p_channel_tv") == "Y")),
                      left_on = "ss_sold_promo_sk",
                      right_on = "p_promo_sk")
                .select(pl.sum("ss_pricing_ext_sales_price")
                        .alias("promotions")),
            ["ss_q61"], tablereader)

        total = ExecutionNode("total",
            lambda ss_q61: ss_q61
                .select(pl.sum("ss_pricing_ext_sales_price")
                        .alias("total")),
            ["ss_q61"], tablereader)

        query61_result = ExecutionNode("query61_result",
            lambda promotions, total: promotions.hstack(total)
                .select([(pl.col("promotions") /
                          pl.col("total") * 100).alias("ratio"),
                          pl.col("promotions"), pl.col("total")]),
            ["promotions", "total"], tablereader)

        job2_nodes = [category_q33, category_q56, category_q60, ss, cs, ws,
                     ss_q33, cs_q33, ws_q33, ss_q56, cs_q56, ws_q56, ss_q60,
                     cs_q60, ws_q60, tmp_q33, tmp_q56, tmp_q60,
                     query33_result, query56_result, query60_result,
                     category_q61, ss_q61, promotions, total, query61_result]

        return job2_nodes, tablereader

    # Job 3 (Queries 2, 59, 74, 75)-------------------------------------------
    if job_num == 3:

        # Query 2
        stack = ExecutionNode("stack",
            lambda ss_base, cs_base, ws_base: ss_base
                .select([pl.col("ss_sold_date_sk").alias("sold_date_sk"),
                         pl.col("ss_pricing_sales_price").
                         alias("sales_price"),
                         pl.col("ss_sold_store_sk").alias("store_sk")])
                .vstack(cs_base
                        .select([pl.col("cs_sold_date_sk")
                                 .alias("sold_date_sk"),
                                 pl.col("cs_pricing_ext_sales_price")
                                 .alias("sales_price"),
                                 pl.lit(-1).cast(pl.datatypes.Int64)
                                 .alias("store_sk")]))
                .vstack(ws_base
                        .select([pl.col("ws_sold_date_sk")
                                 .alias("sold_date_sk"),
                                 pl.col("ws_pricing_ext_sales_price")
                                 .alias("sales_price"),
                                 pl.lit(-1).cast(pl.datatypes.Int64)
                                 .alias("store_sk")]))
                .select([pl.col("sold_date_sk"), pl.col("sales_price"),
                         pl.col("store_sk")]),
            ["ss_base", "cs_base", "ws_base"], tablereader)

        wswscs = ExecutionNode("wswscs",
            lambda stack: stack.filter(pl.col("store_sk") == -1).distinct()
                .join(tablereader.read_table("date"),
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
            ["stack"], tablereader)

        wswscs_2000 = ExecutionNode("wswscs_2000",
            lambda wswscs: wswscs
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2001), on = "d_week_seq")
                .select([pl.col("d_week_seq").alias("d_week_seq1"),
                         pl.col("sun_sales").alias("sun_sales1"),
                         pl.col("mon_sales").alias("mon_sales1"),
                         pl.col("tue_sales").alias("tue_sales1"),
                         pl.col("wed_sales").alias("wed_sales1"),
                         pl.col("thu_sales").alias("thu_sales1"),
                         pl.col("fri_sales").alias("fri_sales1"),
                         pl.col("sat_sales").alias("sat_sales1")]),
            ["wswscs"], tablereader)

        wswscs_2001 = ExecutionNode("wswscs_2001",
            lambda wswscs: wswscs
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2002), on = "d_week_seq")
                .select([pl.col("d_week_seq").map(lambda x: x - 53)
                         .alias("d_week_seq2"),
                         pl.col("sun_sales").alias("sun_sales2"),
                         pl.col("mon_sales").alias("mon_sales2"),
                         pl.col("tue_sales").alias("tue_sales2"),
                         pl.col("wed_sales").alias("wed_sales2"),
                         pl.col("thu_sales").alias("thu_sales2"),
                         pl.col("fri_sales").alias("fri_sales2"),
                         pl.col("sat_sales").alias("sat_sales2")]),
            ["wswscs"], tablereader)

        query2_result = ExecutionNode("query2_result",
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
            ["wswscs_2000", "wswscs_2001"], tablereader)
            
        query2_nodes = [stack, wswscs, wswscs_2000, wswscs_2001, query2_result]

        # Query 59
        wss = ExecutionNode("wss",
            lambda stack: stack.filter(pl.col("store_sk") != -1)
                .join(tablereader.read_table("date"),
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
                .groupby(["d_week_seq", "store_sk"])
                .agg([pl.sum("sun_sales1").alias("sun_sales"),
                      pl.sum("mon_sales1").alias("mon_sales"),
                      pl.sum("tue_sales1").alias("tue_sales"),
                      pl.sum("wed_sales1").alias("wed_sales"),
                      pl.sum("thu_sales1").alias("thu_sales"),
                      pl.sum("fri_sales1").alias("fri_sales"),
                      pl.sum("sat_sales1").alias("sat_sales")])
                .select([pl.col("d_week_seq"), pl.col("store_sk"),
                         pl.col("sun_sales"), pl.col("mon_sales"),
                         pl.col("tue_sales"), pl.col("wed_sales"),
                         pl.col("thu_sales"), pl.col("fri_sales"),
                         pl.col("sat_sales")]),
            ["stack"], tablereader)

        wss_2000 = ExecutionNode("wss_2000",
            lambda wss: wss
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2001), on = "d_week_seq")
                .join(tablereader.read_table("store"),
                      left_on = "store_sk", right_on = "w_store_sk")
                .select([pl.col("w_store_name").alias("store_name1"),
                         pl.col("w_store_id").alias("store_id1"),
                         pl.col("d_week_seq").alias("d_week_seq1"),
                         pl.col("sun_sales").alias("sun_sales1"),
                         pl.col("mon_sales").alias("mon_sales1"),
                         pl.col("tue_sales").alias("tue_sales1"),
                         pl.col("wed_sales").alias("wed_sales1"),
                         pl.col("thu_sales").alias("thu_sales1"),
                         pl.col("fri_sales").alias("fri_sales1"),
                         pl.col("sat_sales").alias("sat_sales1")]),
            ["wss"], tablereader)

        wss_2001 = ExecutionNode("wss_2001",
            lambda wss: wss
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2002), on = "d_week_seq")
                .join(tablereader.read_table("store"),
                      left_on = "store_sk", right_on = "w_store_sk")
                .select([pl.col("w_store_name").alias("store_name2"),
                         pl.col("w_store_id").alias("store_id2"),
                         pl.col("d_week_seq").map(lambda x: x - 53)
                         .alias("d_week_seq2"),
                         pl.col("sun_sales").alias("sun_sales2"),
                         pl.col("mon_sales").alias("mon_sales2"),
                         pl.col("tue_sales").alias("tue_sales2"),
                         pl.col("wed_sales").alias("wed_sales2"),
                         pl.col("thu_sales").alias("thu_sales2"),
                         pl.col("fri_sales").alias("fri_sales2"),
                         pl.col("sat_sales").alias("sat_sales2")]),
            ["wss"], tablereader)

        query59_result = ExecutionNode("query59_result",
            lambda wss_2000, wss_2001: wss_2000
                .join(wss_2001, left_on = ["d_week_seq1", "store_id1"],
                      right_on = ["d_week_seq2", "store_id2"])
                .select([pl.col("store_name1"), pl.col("store_id1"),
                         pl.col("d_week_seq1"),
                         pl.col("sun_sales1") / pl.col("sun_sales2"),
                         pl.col("mon_sales1") / pl.col("mon_sales2"),
                         pl.col("tue_sales1") / pl.col("tue_sales2"),
                         pl.col("wed_sales1") / pl.col("wed_sales2"),
                         pl.col("thu_sales1") / pl.col("thu_sales2"),
                         pl.col("fri_sales1") / pl.col("fri_sales2"),
                         pl.col("sat_sales1") / pl.col("sat_sales2")])
                .sort(["store_name1", "store_id1", "d_week_seq1"])
                .limit(100),
            ["wss_2000", "wss_2001"], tablereader)

        query59_nodes = [wss, wss_2000, wss_2001, query59_result]

        # Query 74
        ss_base = ExecutionNode("ss_base",
            lambda: tablereader.read_table("store_sales")
                .select([pl.col("ss_sold_item_sk"), pl.col("ss_sold_date_sk"),
                         pl.col("ss_ticket_number"),
                         pl.col("ss_pricing_quantity"),
                         pl.col("ss_pricing_net_paid"),
                         pl.col("ss_pricing_ext_sales_price"),
                         pl.col("ss_sold_customer_sk"),
                         pl.col("ss_pricing_sales_price"),
                         pl.col("ss_sold_store_sk")]),
            [], tablereader)

        ws_base = ExecutionNode("ws_base",
            lambda: tablereader.read_table("web_sales")
                .select([pl.col("ws_item_sk"), pl.col("ws_sold_date_sk"),
                         pl.col("ws_order_number"),
                         pl.col("ws_pricing_quantity"),
                         pl.col("ws_pricing_net_paid"),
                         pl.col("ws_pricing_ext_sales_price"),
                         pl.col("ws_bill_customer_sk")]),
            [], tablereader)

        cs_base = ExecutionNode("cs_base",
            lambda: tablereader.read_table("catalog_sales")
                .select([pl.col("cs_sold_item_sk"), pl.col("cs_sold_date_sk"),
                         pl.col("cs_order_number"),
                         pl.col("cs_pricing_quantity"),
                         pl.col("cs_pricing_ext_sales_price")]),
            [], tablereader)

        ws_q74 = ExecutionNode("ws_q74",
            lambda ws_base: ws_base
                .join(tablereader.read_table("customer"),
                      left_on = "ws_bill_customer_sk",
                      right_on = "c_customer_sk")
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2000) |
                              (pl.col("d_year") == 2001)),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .groupby(["c_customer_id", "c_first_name", "c_last_name",
                          "d_year"])
                .agg(pl.sum("ws_pricing_net_paid").alias("year_total"))
                .select([pl.col("c_customer_id").alias("customer_id"),
                         pl.col("c_first_name").alias("customer_first_name"),
                         pl.col("c_last_name").alias("customer_last_name"),
                         pl.col("d_year").alias("year"),
                         pl.col("year_total"), pl.lit("w").alias("sale_type")]),
            ["ws_base"], tablereader)

        ss_q74 = ExecutionNode("ss_q74",
            lambda ss_base: ss_base
                .join(tablereader.read_table("customer"),
                      left_on = "ss_sold_customer_sk",
                      right_on = "c_customer_sk")
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2000) |
                              (pl.col("d_year") == 2001)),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .groupby(["c_customer_id", "c_first_name", "c_last_name",
                          "d_year"])
                .agg(pl.sum("ss_pricing_net_paid").alias("year_total"))
                .select([pl.col("c_customer_id").alias("customer_id"),
                         pl.col("c_first_name").alias("customer_first_name"),
                         pl.col("c_last_name").alias("customer_last_name"),
                         pl.col("d_year").alias("year"),
                         pl.col("year_total"), pl.lit("s").alias("sale_type")]),
            ["ss_base"], tablereader)

        year_total = ExecutionNode("year_total",
            lambda ws_q74, ss_q74: ss_q74.vstack(ws_q74).distinct(),
            ["ws_q74", "ss_q74"], tablereader) 

        t_s_firstyear = ExecutionNode("t_s_firstyear",
            lambda year_total: year_total
                .filter(pl.col("sale_type") == "s")
                .filter(pl.col("year") == 2000)
                .filter(pl.col("year_total") > 0)
                .select([pl.col("customer_id"),
                         pl.col("customer_first_name"),
                         pl.col("customer_last_name"),
                         pl.col("year_total").alias("t_s_f_year_total")]),
            ["year_total"], tablereader)

        t_s_secyear = ExecutionNode("t_s_secyear",
            lambda year_total: year_total
                .filter(pl.col("sale_type") == "s")
                .filter(pl.col("year") == 2001)
                .select([pl.col("customer_id"),
                         pl.col("year_total").alias("t_s_s_year_total")]),
            ["year_total"], tablereader)

        t_w_firstyear = ExecutionNode("t_w_firstyear",
            lambda year_total: year_total
                .filter(pl.col("sale_type") == "w")
                .filter(pl.col("year") == 2000)
                .filter(pl.col("year_total") > 0)
                .select([pl.col("customer_id"),
                         pl.col("year_total").alias("t_w_f_year_total")]),
            ["year_total"], tablereader)

        t_w_secyear = ExecutionNode("t_w_secyear",
            lambda year_total: year_total
                .filter(pl.col("sale_type") == "w")
                .filter(pl.col("year") == 2001)
                .select([pl.col("customer_id"),
                         pl.col("year_total").alias("t_w_s_year_total")]),
            ["year_total"], tablereader)

        query74_result = ExecutionNode("query74_result",
            lambda t_s_firstyear, t_s_secyear,
                                t_w_firstyear, t_w_secyear: t_s_firstyear
                .join(t_s_secyear, on = "customer_id")
                .join(t_w_firstyear, on = "customer_id")
                .join(t_w_secyear, on = "customer_id")
                .filter((pl.col("t_w_s_year_total") /
                         pl.col("t_w_f_year_total")) >
                        (pl.col("t_s_s_year_total") /
                         pl.col("t_s_f_year_total")))
                .select([pl.col("customer_id"),
                         pl.col("customer_first_name"),
                         pl.col("customer_last_name")])
                .sort(["customer_id", "customer_first_name",
                       "customer_last_name"]),
            ["t_s_firstyear", "t_s_secyear", "t_w_firstyear", "t_w_secyear"],
            tablereader)

        query74_nodes = [ss_base, ws_base, cs_base, ws_q74, ss_q74, year_total,
                         t_s_firstyear, t_s_secyear, t_w_firstyear, t_w_secyear,
                         query74_result]

        # Query 75
        ws_q75 = ExecutionNode("ws_q75",
            lambda ws_base: ws_base
                .join(tablereader.read_table("item"),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date"),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("web_returns"),
                      left_on = ["ws_order_number", "ws_item_sk"],
                      right_on = ["wr_order_number", "wr_item_sk"],
                      how = "left")
                .fill_null(0)
                .select([pl.col("d_year"), pl.col("i_brand_id"),
                         pl.col("i_class_id"), pl.col("i_category_id"),
                         pl.col("i_manufact_id"),
                         (pl.col("ws_pricing_quantity") -
                          pl.col("wr_pricing_quantity")).alias("sales_cnt"),
                         (pl.col("ws_pricing_ext_sales_price") -
                          pl.col("wr_pricing_reversed_charge"))
                         .alias("sales_amt")]),
            ["ws_base"], tablereader)

        ss_q75 = ExecutionNode("ss_q75",
            lambda ss_base: ss_base
                .join(tablereader.read_table("item"),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date"),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("store_returns"),
                      left_on = ["ss_ticket_number", "ss_sold_item_sk"],
                      right_on = ["sr_ticket_number", "sr_item_sk"],
                      how = "left")
                .fill_null(0)
                .select([pl.col("d_year"), pl.col("i_brand_id"),
                         pl.col("i_class_id"), pl.col("i_category_id"),
                         pl.col("i_manufact_id"),
                         (pl.col("ss_pricing_quantity") -
                          pl.col("sr_pricing_quantity")).alias("sales_cnt"),
                         (pl.col("ss_pricing_ext_sales_price") -
                          pl.col("sr_pricing_reversed_charge"))
                         .alias("sales_amt")]),
            ["ss_base"], tablereader)

        cs_q75 = ExecutionNode("cs_q75",
            lambda cs_base: cs_base
                .join(tablereader.read_table("item"),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("date"),
                      left_on = "cs_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("catalog_returns"),
                      left_on = ["cs_order_number", "cs_sold_item_sk"],
                      right_on = ["cr_order_number", "cr_item_sk"],
                      how = "left")
                .fill_null(0)
                .select([pl.col("d_year"), pl.col("i_brand_id"),
                         pl.col("i_class_id"), pl.col("i_category_id"),
                         pl.col("i_manufact_id"),
                         (pl.col("cs_pricing_quantity") -
                          pl.col("cr_pricing_quantity"))
                         .alias("sales_cnt"),
                         (pl.col("cs_pricing_ext_sales_price") -
                          pl.col("cr_pricing_reversed_charge"))
                         .alias("sales_amt")]),
            ["cs_base"], tablereader)

        sales_detail = ExecutionNode("sales_detail",
            lambda ws_q75, ss_q75, cs_q75: ws_q75.vstack(ss_q75)
                .vstack(cs_q75),
            ["ws_q75", "ss_q75", "cs_q75"], tablereader)

        curr_yr = ExecutionNode("curr_yr",
            lambda sales_detail: sales_detail
                .filter(pl.col("d_year") == 2001)
                .groupby(["d_year", "i_brand_id", "i_class_id",
                          "i_category_id", "i_manufact_id"])
                .agg([pl.sum("sales_cnt").alias("sales_cnt_curr"),
                      pl.sum("sales_amt").alias("sales_amt_curr")])
                .select([pl.col("d_year").alias("year"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("i_manufact_id"),
                         pl.col("sales_cnt_curr"),
                         pl.col("sales_amt_curr")]),
            ["sales_detail"], tablereader)

        prev_yr = ExecutionNode("prev_yr",
            lambda sales_detail: sales_detail
                .filter(pl.col("d_year") == 2000)
                .groupby(["d_year", "i_brand_id", "i_class_id",
                          "i_category_id", "i_manufact_id"])
                .agg([pl.sum("sales_cnt").alias("sales_cnt_prev"),
                      pl.sum("sales_amt").alias("sales_amt_prev")])
                .select([pl.col("d_year").alias("prev_year"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("i_manufact_id"),
                         pl.col("sales_cnt_prev"),
                         pl.col("sales_amt_prev")]),
            ["sales_detail"], tablereader)

        query75_result = ExecutionNode("query75_result",
            lambda curr_yr, prev_yr: curr_yr
                .join(prev_yr,
                      on = ["i_brand_id", "i_class_id", "i_category_id",
                            "i_manufact_id"])
                .filter(pl.col("sales_cnt_curr") /
                        pl.col("sales_cnt_prev") < 0.9)
                .select([pl.col("prev_year"), pl.col("year"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("i_manufact_id"),
                         pl.col("sales_cnt_prev"), pl.col("sales_cnt_curr"),
                         (pl.col("sales_cnt_curr") - pl.col("sales_cnt_prev"))
                         .alias("sales_cnt_diff"),
                         (pl.col("sales_amt_curr") - pl.col("sales_amt_prev"))
                         .alias("sales_amt_diff")])
                .sort(["sales_cnt_diff", "sales_amt_diff"]),
            ["curr_yr", "prev_yr"], tablereader)

        query75_nodes = [ws_q75, ss_q75, cs_q75, sales_detail, curr_yr, prev_yr,
                         query75_result]

        job3_nodes = query2_nodes + query59_nodes + query74_nodes + \
                        query75_nodes

        return job3_nodes, tablereader

    # Job 4 (Queries 44, 49)----------------------------------------------------
    if job_num == 4:
        ss_base = ExecutionNode("ss_base",
            lambda: tablereader.read_table("store_sales")
                .select([pl.col("ss_sold_item_sk"),
                         pl.col("ss_sold_date_sk"),
                         pl.col("ss_pricing_net_profit"),
                         pl.col("ss_pricing_quantity"),
                         pl.col("ss_pricing_net_paid"),
                         pl.col("ss_ticket_number")]),
            [], tablereader)                                

        # Query 44
        ss_by_item = ExecutionNode("ss_by_item",
            lambda ss_base: ss_base
                .groupby(pl.col("ss_sold_item_sk"))
                .agg(pl.avg("ss_pricing_net_profit").alias("avg_profit")),
            ["ss_base"], tablereader)
        
        avg = ExecutionNode("avg",
            lambda ss_by_item: ss_by_item
                .select(pl.avg("avg_profit").alias("avg_avg_profit")),
            ["ss_by_item"], tablereader)

        v1 = ExecutionNode("v1",
            lambda ss_by_item, avg: ss_by_item
                .filter(pl.col("avg_profit") > 0.9 *
                        avg["avg_avg_profit"].to_list()[0])
                .select([pl.col("ss_sold_item_sk").alias("item_sk"),
                         pl.col("avg_profit").alias("rank_col")]),
            ["ss_by_item", "avg"], tablereader)

        v2 = ExecutionNode("v2",
            lambda ss_by_item, avg: ss_by_item
                .filter(pl.col("avg_profit") > 0.9 *
                        avg["avg_avg_profit"].to_list()[0])
                .select([pl.col("ss_sold_item_sk").alias("item_sk"),
                         pl.col("avg_profit").alias("rank_col")]),
            ["ss_by_item", "avg"], tablereader)

        v11 = ExecutionNode("v11",
            lambda v1: v1
                .select([pl.col("rank_col").rank(reverse = False).alias("rnk"),
                         pl.col("item_sk")]),
            ["v1"], tablereader)

        v21 = ExecutionNode("v21",
            lambda v2: v2
                .select([pl.col("rank_col").rank(reverse = True).alias("rnk"),
                         pl.col("item_sk")]),
            ["v2"], tablereader)

        ascending = ExecutionNode("ascending",
            lambda v11: v11
                .filter(pl.col("rnk") < 11),
            ["v11"], tablereader)

        descending = ExecutionNode("descending",
            lambda v21: v21
                .filter(pl.col("rnk") < 11),
            ["v21"], tablereader)

        query44_result = ExecutionNode("query44_result",
            lambda ascending, descending: ascending
                .join(tablereader.read_table("item"),
                      left_on = "item_sk", right_on = "i_item_sk")
                .select([pl.col("rnk"),
                         pl.col("i_product_name").alias("best_performing")])
                .join(descending
                      .join(tablereader.read_table("item"),
                            left_on = "item_sk", right_on = "i_item_sk")
                      .select([pl.col("rnk"),
                               pl.col("i_product_name")
                               .alias("worst_performing")]), on = "rnk")
                .select([pl.col("rnk"), pl.col("best_performing"),
                         pl.col("worst_performing")])
                .sort("rnk"),
            ["ascending", "descending"], tablereader)

        query44_nodes = [ss_base, ss_by_item, avg, v1, v2, v11, v21,
                         ascending, descending, query44_result]

        # Query 49
        in_store = ExecutionNode("in_store",
            lambda ss_base: ss_base
                .filter(pl.col("ss_pricing_net_profit") > 1)
                .filter(pl.col("ss_pricing_quantity") > 0)
                .filter(pl.col("ss_pricing_net_paid") > 0)
                .join(tablereader.read_table("store_returns"),
                      left_on = ["ss_ticket_number", "ss_sold_item_sk"],
                      right_on = ["sr_ticket_number", "sr_item_sk"],
                      how = "left")
                .filter(pl.col("sr_pricing_reversed_charge") > 1)
                .fill_null(0)
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2001)
                      .filter(pl.col("d_moy") == 12),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .groupby("ss_sold_item_sk")
                .agg([pl.sum("ss_pricing_quantity").alias("ssq"),
                      pl.sum("sr_pricing_quantity").alias("srq"),
                      pl.sum("ss_pricing_net_paid").alias("ssa"),
                      pl.sum("sr_pricing_reversed_charge").alias("sra")])
                .select([(pl.col("srq") / pl.col("ssq")).alias("return_ratio"),
                         (pl.col("sra") / pl.col("ssa")).alias("currency_ratio"),
                         pl.col("ss_sold_item_sk").alias("item")]),
            ["ss_base"], tablereader)

        in_cat = ExecutionNode("in_cat",
            lambda: tablereader.read_table("catalog_sales")
                .filter(pl.col("cs_pricing_net_profit") > 1)
                .filter(pl.col("cs_pricing_quantity") > 0)
                .filter(pl.col("cs_pricing_net_paid") > 0)
                .join(tablereader.read_table("catalog_returns"),
                      left_on = ["cs_order_number", "cs_sold_item_sk"],
                      right_on = ["cr_order_number", "cr_item_sk"],
                      how = "left")
                .filter(pl.col("cr_pricing_reversed_charge") > 1)
                .fill_null(0)
                .join(tablereader.read_table("date")
                      .filter((pl.col("d_year") == 2001) &
                              (pl.col("d_moy") == 12)),
                      left_on = "cs_sold_date_sk", right_on = "d_date_sk")
                .groupby("cs_sold_item_sk")
                .agg([pl.sum("cs_pricing_quantity").alias("csq"),
                      pl.sum("cr_pricing_quantity").alias("crq"),
                      pl.sum("cs_pricing_net_paid").alias("csa"),
                      pl.sum("cr_pricing_reversed_charge").alias("cra")])
                .select([(pl.col("crq") / pl.col("csq")).alias("return_ratio"),
                         (pl.col("cra") / pl.col("csa")).alias("currency_ratio"),
                         pl.col("cs_sold_item_sk").alias("item")]),
            [], tablereader)

        in_web = ExecutionNode("in_web",
            lambda: tablereader.read_table("web_sales")
                .filter(pl.col("ws_pricing_net_profit") > 1)
                .filter(pl.col("ws_pricing_quantity") > 0)
                .filter(pl.col("ws_pricing_net_paid") > 0)
                .join(tablereader.read_table("web_returns"),
                      left_on = ["ws_order_number", "ws_item_sk"],
                      right_on = ["wr_order_number", "wr_item_sk"],
                      how = "left")
                .filter(pl.col("wr_pricing_reversed_charge") > 1)
                .fill_null(0)
                .join(tablereader.read_table("date")
                      .filter(pl.col("d_year") == 2001)
                      .filter(pl.col("d_moy") == 12),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .groupby("ws_item_sk")
                .agg([pl.sum("ws_pricing_quantity").alias("wsq"),
                      pl.sum("wr_pricing_quantity").alias("wrq"),
                      pl.sum("ws_pricing_net_paid").alias("wsa"),
                      pl.sum("wr_pricing_reversed_charge").alias("wra")])
                .select([(pl.col("wrq") / pl.col("wsq")).alias("return_ratio"),
                         (pl.col("wra") / pl.col("wsa")).alias("currency_ratio"),
                         pl.col("ws_item_sk").alias("item")]),
            [], tablereader)

        store = ExecutionNode("store",
            lambda in_store: in_store
                .select([pl.col("item"), pl.col("return_ratio"),
                         pl.col("currency_ratio"),
                         pl.col("return_ratio").rank(reverse = True)
                         .alias("return_rank"),
                         pl.col("currency_ratio").rank(reverse = True)
                         .alias("currency_rank")]),
            ["in_store"], tablereader)

        catalog = ExecutionNode("catalog",
            lambda in_cat: in_cat
                .select([pl.col("item"), pl.col("return_ratio"),
                         pl.col("currency_ratio"),
                         pl.col("return_ratio").rank(reverse = True)
                         .alias("return_rank"),
                         pl.col("currency_ratio").rank(reverse = True)
                         .alias("currency_rank")]),
            ["in_cat"], tablereader)

        web = ExecutionNode("web",
            lambda in_web: in_web
                .select([pl.col("item"), pl.col("return_ratio"),
                         pl.col("currency_ratio"),
                         pl.col("return_ratio").rank(reverse = True)
                         .alias("return_rank"),
                         pl.col("currency_ratio").rank(reverse = True)
                         .alias("currency_rank")]),
            ["in_web"], tablereader)

        store_filter = ExecutionNode("store_filter",
            lambda store: store
                .filter((pl.col("currency_rank") <= 10) |
                        (pl.col("return_rank") <= 10))
                .select([pl.lit("store").alias("channel"),
                         pl.col("item"), pl.col("return_ratio"),
                         pl.col("return_rank"), pl.col("currency_rank")]),
            ["store"], tablereader)

        catalog_filter = ExecutionNode("catalog_filter",
            lambda catalog: catalog
                .filter((pl.col("currency_rank") <= 10) |
                        (pl.col("return_rank") <= 10))
                .select([pl.lit("catalog").alias("channel"),
                         pl.col("item"), pl.col("return_ratio"),
                         pl.col("return_rank"), pl.col("currency_rank")]),
            ["catalog"], tablereader)

        web_filter = ExecutionNode("web_filter",
            lambda web: web
                .filter((pl.col("currency_rank") <= 10) |
                        (pl.col("return_rank") <= 10))
                .select([pl.lit("web").alias("channel"),
                         pl.col("item"), pl.col("return_ratio"),
                         pl.col("return_rank"), pl.col("currency_rank")]),
            ["web"], tablereader)
                
        query49_result = ExecutionNode("query49_result",
            lambda store_filter, catalog_filter, web_filter: store_filter
                .vstack(catalog_filter).vstack(web_filter)
                .select([pl.col("channel"),
                         pl.col("item"), pl.col("return_ratio"),
                         pl.col("return_rank"), pl.col("currency_rank")])
                .sort(["channel", "return_rank", "currency_rank", "item"])
                .limit(100),
            ["store_filter", "catalog_filter", "web_filter"], tablereader)

        query49_nodes = [in_store, in_cat, in_web, store, catalog, web, store_filter,
                         catalog_filter, web_filter, query49_result]

        job4_nodes = query44_nodes + query49_nodes

        return job4_nodes, tablereader

    # Job 5 (Queries 14, 23)----------------------------------------------------
    if job_num == 5:

        # Query 14
        ss_base = ExecutionNode("ss_base",
            lambda: tablereader.read_table("store_sales")
                .join(tablereader.read_table("date")
                      .select([pl.col("d_year"), pl.col("d_date_sk"),
                               pl.col("d_moy"), pl.col("d_week_seq"),
                               pl.col("d_date")])
                      .filter((pl.col("d_year") >= 1999) &
                              (pl.col("d_year") <= 2000)),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .select([pl.col("i_item_sk"), pl.col("i_brand_id"),
                               pl.col("i_class_id"), pl.col("i_category_id")]),
                      left_on = "ss_sold_item_sk", right_on = "i_item_sk"),
            [], tablereader)

        cs_base = ExecutionNode("cs_base",
            lambda: tablereader.read_table("catalog_sales")
                .join(tablereader.read_table("date")
                      .select([pl.col("d_year"), pl.col("d_date_sk"),
                               pl.col("d_moy"), pl.col("d_week_seq"),
                               pl.col("d_date")])
                      .filter((pl.col("d_year") >= 1999) &
                              (pl.col("d_year") <= 2000)),
                      left_on = "cs_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .select([pl.col("i_item_sk"), pl.col("i_brand_id"),
                               pl.col("i_class_id"), pl.col("i_category_id")]),
                      left_on = "cs_sold_item_sk", right_on = "i_item_sk"),
            [], tablereader)

        ws_base = ExecutionNode("ws_base",
            lambda: tablereader.read_table("web_sales")
                .join(tablereader.read_table("date")
                      .select([pl.col("d_year"), pl.col("d_date_sk"),
                               pl.col("d_moy"), pl.col("d_week_seq"),
                               pl.col("d_date")])
                      .filter((pl.col("d_year") >= 1999) &
                              (pl.col("d_year") <= 2000)),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item")
                      .select([pl.col("i_item_sk"), pl.col("i_brand_id"),
                               pl.col("i_class_id"), pl.col("i_category_id")]),
                      left_on = "ws_item_sk", right_on = "i_item_sk"),
            [], tablereader)

        ssi_q14 = ExecutionNode("ssi_q14",
            lambda ss_base: ss_base
                .select([pl.col("i_brand_id").alias("brand_id"),
                         pl.col("i_class_id").alias("class_id"),
                         pl.col("i_category_id").alias("category_id")]),
            ["ss_base"], tablereader)

        csi_q14 = ExecutionNode("csi_q14",
            lambda cs_base: cs_base
                .select([pl.col("i_brand_id").alias("brand_id"),
                         pl.col("i_class_id").alias("class_id"),
                         pl.col("i_category_id").alias("category_id")]),
            ["cs_base"], tablereader)

        wsi_q14 = ExecutionNode("wsi_q14",
            lambda ws_base: ws_base
                .select([pl.col("i_brand_id").alias("brand_id"),
                         pl.col("i_class_id").alias("class_id"),
                         pl.col("i_category_id").alias("category_id")]),
            ["ws_base"], tablereader)

        intersect = ExecutionNode("intersect",
            lambda ssi_q14, csi_q14, wsi_q14: ssi_q14.distinct()
                .join(csi_q14.distinct(),
                      on = ["brand_id", "class_id", "category_id"])
                .join(wsi_q14.distinct(),
                      on = ["brand_id", "class_id", "category_id"]),
            ["ssi_q14", "csi_q14", "wsi_q14"], tablereader)

        cross_items = ExecutionNode("cross_items",
            lambda intersect: intersect
                .join(tablereader.read_table("item"),
                      left_on = ["brand_id", "class_id", "category_id"],
                      right_on = ["i_brand_id", "i_class_id", "i_category_id"])
                .select(pl.col("i_item_sk").alias("ss_item_sk")),
            ["intersect"], tablereader)

        ssu_q14 = ExecutionNode("ssu_q14",
            lambda ss_base: ss_base
                .select([pl.col("ss_pricing_quantity").alias("quantity"),
                         pl.col("ss_pricing_list_price").alias("list_price")]),
            ["ss_base"], tablereader)

        csu_q14 = ExecutionNode("csu_q14",
            lambda cs_base: cs_base
                .select([pl.col("cs_pricing_quantity").alias("quantity"),
                         pl.col("cs_pricing_list_price").alias("list_price")]),
            ["cs_base"], tablereader)

        wsu_q14 = ExecutionNode("wsu_q14",
            lambda ws_base: ws_base
                .select([pl.col("ws_pricing_quantity").alias("quantity"),
                         pl.col("ws_pricing_list_price").alias("list_price")]),
            ["ws_base"], tablereader)

        x = ExecutionNode("x",
            lambda ssu_q14, csu_q14, wsu_q14: ssu_q14
                .vstack(csu_q14).vstack(wsu_q14),
            ["ssu_q14", "csu_q14", "wsu_q14"], tablereader)

        avg_sales = ExecutionNode("avg_sales",
            lambda x: x
                .with_columns((pl.col("quantity") *
                               pl.col("list_price")).alias("temp_product"))
                .select(pl.avg("temp_product").alias("average_sales")),
            ["x"], tablereader)

        ss_iteration1 = ExecutionNode("ss_iteration1",
            lambda ss_base, cross_items, avg_sales: ss_base
                .filter(pl.col("d_year") == 2000)
                .filter(pl.col("d_moy") == 11)
                .filter(pl.col("ss_sold_item_sk")
                              .is_in(cross_items["ss_item_sk"].to_list()))
                .with_columns((pl.col("ss_pricing_quantity") *
                               pl.col("ss_pricing_list_price"))
                              .alias("sales_product"))
                .groupby(["i_brand_id", "i_class_id", "i_category_id"])
                .agg([pl.sum("sales_product").alias("sales"),
                      pl.count().alias("number_sales")])
                .filter(pl.col("sales") > avg_sales["average_sales"]
                        .to_list()[0])
                .select([pl.lit("store").alias("channel"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("sales"),
                         pl.col("number_sales")]),
            ["ss_base", "cross_items", "avg_sales"], tablereader)

        cs_iteration1 = ExecutionNode("cs_iteration1",
            lambda cs_base, cross_items, avg_sales: cs_base
                .filter(pl.col("d_year") == 2000)
                .filter(pl.col("d_moy") == 11)
                .filter(pl.col("cs_sold_item_sk")
                              .is_in(cross_items["ss_item_sk"].to_list()))
                .with_columns((pl.col("cs_pricing_quantity") *
                               pl.col("cs_pricing_list_price"))
                              .alias("sales_product"))
                .groupby(["i_brand_id", "i_class_id", "i_category_id"])
                .agg([pl.sum("sales_product").alias("sales"),
                      pl.count().alias("number_sales")])
                .filter(pl.col("sales") > avg_sales["average_sales"]
                        .to_list()[0])
                .select([pl.lit("catalog").alias("channel"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("sales"),
                         pl.col("number_sales")]),
            ["cs_base", "cross_items", "avg_sales"], tablereader)

        ws_iteration1 = ExecutionNode("ws_iteration1",
            lambda ws_base, cross_items, avg_sales: ws_base
                .filter(pl.col("d_year") == 2000)
                .filter(pl.col("d_moy") == 11)
                .filter(pl.col("ws_item_sk")
                              .is_in(cross_items["ss_item_sk"].to_list()))
                .with_columns((pl.col("ws_pricing_quantity") *
                               pl.col("ws_pricing_list_price"))
                              .alias("sales_product"))
                .groupby(["i_brand_id", "i_class_id", "i_category_id"])
                .agg([pl.sum("sales_product").alias("sales"),
                      pl.count().alias("number_sales")])
                .filter(pl.col("sales") > avg_sales["average_sales"]
                        .to_list()[0])
                .select([pl.lit("web").alias("channel"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"), pl.col("sales"),
                         pl.col("number_sales")]),
            ["ws_base", "cross_items", "avg_sales"], tablereader)

        y_iteration1 = ExecutionNode("y_iteration1",
            lambda ss_iteration1, cs_iteration1, ws_iteration1: ss_iteration1
                .vstack(cs_iteration1).vstack(ws_iteration1),
            ["ss_iteration1", "cs_iteration1", "ws_iteration1"], tablereader)

        query14_result1 = ExecutionNode("query14_result1",
            lambda y_iteration1: y_iteration1
                .groupby(["channel", "i_brand_id",
                          "i_class_id", "i_category_id"])
                .agg([pl.sum("sales").alias("sales_sum"),
                      pl.sum("number_sales").alias("number_sales_sum")])
                .select([pl.col("channel"), pl.col("i_brand_id"),
                         pl.col("i_class_id"), pl.col("i_category_id"),
                         pl.col("sales_sum"), pl.col("number_sales_sum")])
                .sort(["channel", "i_brand_id",
                          "i_class_id", "i_category_id"])
                .limit(100),
            ["y_iteration1"], tablereader)

        this_year = ExecutionNode("this_year",
            lambda ss_base, cross_items, avg_sales: ss_base
                .filter(pl.col("d_week_seq") > tablereader
                                                    .read_table("date")
                                     .filter(pl.col("d_year") == 2000)
                                     .filter(pl.col("d_moy") == 12)
                                     .filter(pl.col("d_dom") == 11)
                                     ["d_week_seq"].to_list()[0])
                .filter(pl.col("ss_sold_item_sk")
                              .is_in(cross_items["ss_item_sk"].to_list()))
                .with_columns((pl.col("ss_pricing_quantity") *
                               pl.col("ss_pricing_list_price"))
                              .alias("sales_product"))
                .groupby(["i_brand_id", "i_class_id", "i_category_id"])
                .agg([pl.sum("sales_product").alias("sales"),
                      pl.count().alias("number_sales")])
                .filter(pl.col("sales") > avg_sales["average_sales"]
                        .to_list()[0])
                .select([pl.lit("store").alias("channel"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"),
                         pl.col("sales").alias("ty_sales"),
                         pl.col("number_sales").alias("ty_number_sales")]),
            ["ss_base", "cross_items", "avg_sales"], tablereader)

        last_year = ExecutionNode("last_year",
            lambda ss_base, cross_items, avg_sales: ss_base
                .filter(pl.col("d_week_seq") > tablereader
                                                    .read_table("date")
                                     .filter(pl.col("d_year") == 1999)
                                     .filter(pl.col("d_moy") == 12)
                                     .filter(pl.col("d_dom") == 11)
                                     ["d_week_seq"].to_list()[0])
                .filter(pl.col("ss_sold_item_sk")
                              .is_in(cross_items["ss_item_sk"].to_list()))
                .with_columns((pl.col("ss_pricing_quantity") *
                               pl.col("ss_pricing_list_price"))
                              .alias("sales_product"))
                .groupby(["i_brand_id", "i_class_id", "i_category_id"])
                .agg([pl.sum("sales_product").alias("sales"),
                      pl.count().alias("number_sales")])
                .filter(pl.col("sales") > avg_sales["average_sales"]
                        .to_list()[0])
                .select([pl.lit("store").alias("channel"),
                         pl.col("i_brand_id"), pl.col("i_class_id"),
                         pl.col("i_category_id"),
                         pl.col("sales").alias("ly_sales"),
                         pl.col("number_sales").alias("ly_number_sales")]),
            ["ss_base", "cross_items", "avg_sales"], tablereader)

        query14_result2 = ExecutionNode("query14_result2",
            lambda this_year, last_year: this_year
                .join(last_year,
                      on = ["i_brand_id", "i_class_id", "i_category_id"])
                .select([pl.col("channel").alias("ty_channel"),
                         pl.col("channel").alias("ly_channel"),
                         pl.col("i_brand_id").alias("ty_brand"),
                         pl.col("i_brand_id").alias("ly_brand"),
                         pl.col("i_class_id").alias("ty_class"),
                         pl.col("i_class_id").alias("ly_class"),
                         pl.col("i_category_id").alias("ty_category"),
                         pl.col("i_category_id").alias("ly_category"),
                         pl.col("ty_sales"), pl.col("ly_sales"),
                         pl.col("ty_number_sales"), pl.col("ly_number_sales")])
                .sort(["ty_channel", "ty_brand", "ty_class", "ty_category"])
                .limit(100),
            ["this_year", "last_year"], tablereader)

        query14_nodes = [ss_base, cs_base, ws_base, ssi_q14, csi_q14, wsi_q14,
                         intersect, cross_items, ssu_q14, csu_q14, wsu_q14,
                         x, avg_sales, ss_iteration1, cs_iteration1,
                         ws_iteration1, y_iteration1, query14_result1,
                         this_year, last_year, query14_result2]

        # Query 23
        frequent_ss_items = ExecutionNode("frequent_ss_items",
            lambda ss_base: ss_base
                .with_columns(pl.lit(str(pl.col("i_item_desc"))[:30])
                              .alias("itemdesc"))
                .groupby(["itemdesc", "ss_sold_item_sk", "d_date"])
                .agg(pl.count().alias("cnt"))
                .filter(pl.col("cnt") > 4)
                .select([pl.col("itemdesc"), pl.col("cnt"),
                         pl.col("ss_sold_item_sk").alias("item_sk"),
                         pl.col("d_date").alias("soldate")]),
            ["ss_base"], tablereader)

        max_store_sales = ExecutionNode("max_store_sales",
            lambda ss_base: ss_base
                .join(tablereader.read_table("customer"),
                      left_on = "ss_sold_customer_sk",
                      right_on = "c_customer_sk")
                .with_columns((pl.col("ss_pricing_quantity") *
                               pl.col("ss_pricing_sales_price"))
                              .alias("csales"))
                .groupby(["ss_sold_customer_sk"])
                .agg(pl.sum("csales").alias("csales_sum"))
                .select(pl.max("csales_sum").alias("csales_max")),
            ["ss_base"], tablereader)

        best_ss_customer = ExecutionNode("best_ss_customer",
            lambda ss_base, max_store_sales: ss_base
                .join(tablereader.read_table("customer"),
                      left_on = "ss_sold_customer_sk",
                      right_on = "c_customer_sk")
                .with_columns((pl.col("ss_pricing_quantity") *
                               pl.col("ss_pricing_sales_price"))
                              .alias("csales"))
                .groupby(["ss_sold_customer_sk"])
                .agg(pl.sum("csales").alias("csales_sum"))
                .filter(pl.col("csales_sum") > max_store_sales["csales_max"]
                        .to_list()[0] * 0.6)
                .select([pl.col("csales_sum"), pl.col("ss_sold_customer_sk")]),
            ["ss_base", "max_store_sales"], tablereader)

        css_iteration1 = ExecutionNode("css_iteration1",
            lambda cs_base, frequent_ss_items, best_ss_customer: cs_base
                .filter(pl.col("cs_sold_item_sk")
                        .is_in(frequent_ss_items["item_sk"].to_list()))
                .filter(pl.col("cs_bill_customer_sk")
                        .is_in(best_ss_customer["ss_sold_customer_sk"]
                               .to_list()))
                .select((pl.col("cs_pricing_quantity") *
                         pl.col("cs_pricing_list_price"))
                        .alias("sales")),
            ["cs_base", "frequent_ss_items", "best_ss_customer"], tablereader)

        wss_iteration1 = ExecutionNode("wss_iteration1",
            lambda ws_base, frequent_ss_items, best_ss_customer: ws_base
                .filter(pl.col("ws_item_sk")
                        .is_in(frequent_ss_items["item_sk"].to_list()))
                .filter(pl.col("ws_bill_customer_sk")
                        .is_in(best_ss_customer["ss_sold_customer_sk"]
                               .to_list()))
                .select((pl.col("ws_pricing_quantity") *
                         pl.col("ws_pricing_list_price"))
                        .alias("sales")),
            ["ws_base", "frequent_ss_items", "best_ss_customer"], tablereader)

        stack_iteration1 = ExecutionNode("stack_iteration1",
            lambda css_iteration1, wss_iteration1: css_iteration1
                .vstack(wss_iteration1).distinct(),
            ["css_iteration1", "wss_iteration1"], tablereader)

        query23_result1 = ExecutionNode("query23_result1",
            lambda stack_iteration1: stack_iteration1
                .select(pl.sum("sales").alias("sum_sales")),
            ["stack_iteration1"], tablereader)

        css_iteration2 = ExecutionNode("css_iteration2",
            lambda cs_base, frequent_ss_items, best_ss_customer: cs_base
                .filter(pl.col("cs_sold_item_sk")
                        .is_in(frequent_ss_items["item_sk"].to_list()))
                .filter(pl.col("cs_bill_customer_sk")
                        .is_in(best_ss_customer["ss_sold_customer_sk"]
                               .to_list()))
                .join(tablereader.read_table("customer"),
                      left_on = "cs_bill_customer_sk",
                      right_on = "c_customer_sk")
                .with_columns((pl.col("cs_pricing_quantity") *
                               pl.col("cs_pricing_sales_price"))
                              .alias("sales"))
                .groupby(["c_last_name", "c_first_name"])
                .agg(pl.sum("sales").alias("sales_sum"))
                .select([pl.col("c_last_name"), pl.col("c_first_name"),
                         pl.col("sales_sum")]),
            ["cs_base", "frequent_ss_items", "best_ss_customer"], tablereader)

        wss_iteration2 = ExecutionNode("wss_iteration2",
            lambda ws_base, frequent_ss_items, best_ss_customer: ws_base
                .filter(pl.col("ws_item_sk")
                        .is_in(frequent_ss_items["item_sk"].to_list()))
                .filter(pl.col("ws_bill_customer_sk")
                        .is_in(best_ss_customer["ss_sold_customer_sk"]
                               .to_list()))
                .join(tablereader.read_table("customer"),
                      left_on = "ws_bill_customer_sk",
                      right_on = "c_customer_sk")
                .with_columns((pl.col("ws_pricing_quantity") *
                               pl.col("ws_pricing_sales_price"))
                              .alias("sales"))
                .groupby(["c_last_name", "c_first_name"])
                .agg(pl.sum("sales").alias("sales_sum"))
                .select([pl.col("c_last_name"), pl.col("c_first_name"),
                         pl.col("sales_sum")]),
            ["ws_base", "frequent_ss_items", "best_ss_customer"], tablereader)

        stack_iteration2 = ExecutionNode("stack_iteration2",
            lambda css_iteration2, wss_iteration2: css_iteration2
                .vstack(wss_iteration2).distinct(),
            ["css_iteration2", "wss_iteration2"], tablereader)

        query23_result2 = ExecutionNode("query23_result2",
            lambda stack_iteration1: stack_iteration1
                .select([pl.col("c_last_name"), pl.col("c_first_name"),
                         pl.col("sales_sum")])
                .sort(["c_last_name", "c_first_name", "sales_sum"])
                .limit(100),
            ["stack_iteration2"], tablereader)
        
        query23_nodes = [frequent_ss_items, max_store_sales, best_ss_customer,
                         css_iteration1, wss_iteration1, stack_iteration1,
                         css_iteration2, wss_iteration2, stack_iteration2,
                         query23_result1, query23_result2]

        job5_nodes = query14_nodes + query23_nodes
        
        return job5_nodes, tablereader
