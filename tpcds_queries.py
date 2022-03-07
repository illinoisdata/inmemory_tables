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
                         pl.col("ws_item_sk"), pl.col("ws_order_number")]),
            [], tablereader)

        wr = ExecutionNode("wr",
            lambda: tablereader.read_table("web_returns")
                .join(tablereader.read_table("web_sales"),
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
                         pl.col("wr_pricing_net_loss").alias("net_loss"),
                         pl.col("wr_item_sk"), pl.col("wr_order_number")]),
            [], tablereader)

        salesreturns3 = ExecutionNode("salesreturns3",
            lambda ws, wr: ws
                .drop(["ws_item_sk", "ws_order_number"])
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
                .join(tablereader.read_table("catalog_page"), left_on = "page_sk",
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
                .join(tablereader.read_table("web_site"), left_on = "wsr_web_site_sk",
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
            lambda ssr, csr, wsr: ssr.vstack(csr).vstack(wsr),
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
            lambda: tablereader.read_table("web_sales")
                .join(tablereader.read_table("date").filter(pl.col("d_month_seq") == 1207),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("web_page"),
                      left_on = "ws_web_page_sk", right_on = "wp_page_sk")
                .groupby("ws_web_page_sk")
                .agg([pl.sum("ws_pricing_ext_sales_price").alias("sales"),
                      pl.sum("ws_pricing_net_profit").alias("profit_gain")])
                .select([pl.col("ws_web_page_sk"), pl.col("sales"),
                         pl.col("profit_gain")]),
            [], tablereader)

        wr_groupby = ExecutionNode("wr_groupby",
            lambda: tablereader.read_table("web_returns")
                .join(tablereader.read_table("date").filter(pl.col("d_month_seq") == 1207),
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
                .vstack(cs_cr_join).vstack(ws_wr_join),
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
                .join(tablereader.read_table("date").filter(pl.col("d_month_seq") == 1207),
                      left_on = "ss_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item").filter(pl.col("i_current_price") > 50),
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
                .join(tablereader.read_table("date").filter(pl.col("d_month_seq") == 1207),
                      left_on = "cs_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item").filter(pl.col("i_current_price") > 50),
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
            lambda: tablereader.read_table("web_sales")
                .join(tablereader.read_table("web_returns"),
                      left_on = ["ws_item_sk", "ws_order_number"],
                      right_on = ["wr_item_sk", "wr_order_number"],
                      how = "left")
                .fill_null(0)
                .join(tablereader.read_table("web_site"),
                      left_on = "ws_web_site_sk",
                      right_on = "web_site_sk")
                .join(tablereader.read_table("date").filter(pl.col("d_month_seq") == 1207),
                      left_on = "ws_sold_date_sk", right_on = "d_date_sk")
                .join(tablereader.read_table("item").filter(pl.col("i_current_price") > 50),
                      left_on = "ws_item_sk", right_on = "i_item_sk")
                .join(tablereader.read_table("promotion")
                      .filter(pl.col("p_channel_tv") == "N"),
                      left_on = "ws_promo_sk", right_on = "p_promo_sk")
                .groupby("web_site_id")
                .agg([pl.sum("ws_pricing_ext_sales_price").alias("sales2"),
                      pl.sum("wr_pricing_reversed_charge").alias("returns2"),
                      pl.sum("ws_pricing_net_profit").alias("profit2"),
                      pl.sum("wr_pricing_net_loss").alias("net_loss2")])
                .select([pl.lit("web channel").alias("channel"),
                         pl.col("web_site_id").alias("id"),
                         pl.col("sales2"), pl.col("returns2"),
                         (pl.col("profit2") - pl.col("net_loss2"))
                         .alias("net_profit")]),
            [], tablereader)

        x2 = ExecutionNode("x2",
            lambda ssr2, csr2, wsr2: ssr2.vstack(csr2).vstack(wsr2),
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
                        .is_in(list(category_q33
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ss", "category_q33"], tablereader)

        cs_q33 = ExecutionNode("cs_q33",
            lambda cs, category_q33: cs
                .filter(pl.col("i_manufact_id")
                        .is_in(list(category_q33
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["cs", "category_q33"], tablereader)

        ws_q33 = ExecutionNode("ws_q33",
            lambda ws, category_q33: ws
                .filter(pl.col("i_manufact_id")
                        .is_in(list(category_q33
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ws", "category_q33"], tablereader)

        ss_q56 = ExecutionNode("ss_q56",
            lambda ss, category_q56: ss
                .filter(pl.col("i_item_id")
                        .is_in(list(category_q56
                                    .select(pl.col("i_item_id")))))
                .groupby("i_item_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["ss", "category_q56"], tablereader)

        cs_q56 = ExecutionNode("cs_q56",
            lambda cs, category_q56: cs
                .filter(pl.col("i_item_id")
                        .is_in(list(category_q56
                                    .select(pl.col("i_item_id")))))
                .groupby("i_item_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["cs", "category_q56"], tablereader)

        ws_q56 = ExecutionNode("ws_q56",
            lambda ws, category_q56: ws
                .filter(pl.col("i_item_id")
                        .is_in(list(category_q56
                                    .select(pl.col("i_item_id")))))
                .groupby("i_item_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_item_id"), pl.col("total_sales")]),
            ["ws", "category_q56"], tablereader)

        ss_q60 = ExecutionNode("ss_q60",
            lambda ss, category_q60: ss
                .filter(pl.col("i_manufact_id")
                        .is_in(list(category_q60
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("ss_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ss", "category_q60"], tablereader)

        cs_q60 = ExecutionNode("cs_q60",
            lambda cs, category_q60: cs
                .filter(pl.col("i_manufact_id")
                        .is_in(list(category_q60
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("cs_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["cs", "category_q60"], tablereader)

        ws_q60 = ExecutionNode("ws_q60",
            lambda ws, category_q60: ws
                .filter(pl.col("i_manufact_id")
                        .is_in(list(category_q60
                                    .select(pl.col("i_manufact_id")))))
                .groupby("i_manufact_id")
                .agg(pl.sum("ws_pricing_ext_sales_price")
                     .alias("total_sales"))
                .select([pl.col("i_manufact_id"), pl.col("total_sales")]),
            ["ws", "category_q60"], tablereader)


        tmp_q33 = ExecutionNode("tmp_q33",
            lambda ss_q33, cs_q33, ws_q33: ss_q33.vstack(cs_q33)
                .vstack(ws_q33),
            ["ss_q33", "cs_q33", "ws_q33"], tablereader)

        tmp_q56 = ExecutionNode("tmp_q56",
            lambda ss_q56, cs_q56, ws_q56: ss_q56.vstack(cs_q56)
                .vstack(ws_q56),
            ["ss_q56", "cs_q56", "ws_q56"], tablereader)

        tmp_q60 = ExecutionNode("tmp_q60",
            lambda ss_q60, cs_q60, ws_q60: ss_q60.vstack(cs_q60)
                .vstack(ws_q60),
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
                        .is_in(list(category_q61
                                    .select(pl.col("i_manufact_id")))))
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
        wscs = ExecutionNode("wscs",
            lambda: tablereader.read_table("web_sales")
                 .select([pl.col("ws_sold_date_sk").alias("sold_date_sk"),
                          pl.col("ws_pricing_ext_sales_price")
                          .alias("sales_price")])
                 .vstack(tablereader.read_table("catalog_sales")
                         .select([pl.col("cs_sold_date_sk")
                                  .alias("sold_date_sk"),
                                  pl.col("cs_pricing_ext_sales_price")
                                  .alias("sales_price")]))
                 .select([pl.col("sold_date_sk"), pl.col("sales_price")]),
            [])

        wswscs = ExecutionNode("wswscs",
            lambda wscs: wscs
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
            ["wscs"])

        wswscs_2000 = ExecutionNode("wswscs_2000",
            lambda wswscs: wswscs
                .join(tablereader.read_table("date")
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
                .join(tablereader.read_table("date")
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

        return query2_nodes, tablereader
