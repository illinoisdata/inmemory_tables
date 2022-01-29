from ExecutionNode import *
from utils import *
import polars as pl

"""
A file containing select TPC-DS queries translated into equivalent polars
dataframe operations.
Available queries: 1, 2, 3, 5, 6
"""
def get_tpcds_query_nodes(query_num = 1):
    # Query 1----------------------------------------------------------------
    if query_num == 1:
        store_returns = ExecutionNode("store_returns",
                                      lambda: read_table("store_returns"), [])
        date = ExecutionNode("date", lambda: read_table("date"), [])
        store = ExecutionNode("store", lambda: read_table("store"), [])
        customer = ExecutionNode("customer", lambda: read_table("customer"), [])

        sd_join = ExecutionNode("sd_join",
            lambda s, d: s
                .join(d, left_on = "sr_returned_date_sk",
                      right_on = "d_date_sk"),
            ["store_returns", "date"])

        sd_where = ExecutionNode("sd_where",
            lambda sd_select: sd_select
                .filter(pl.col("d_year") == 2000),
            ["sd_join"])

        sd_select = ExecutionNode("sd_select",
            lambda sd_join: sd_join
                .select(["sr_customer_sk", "sr_store_sk",
                         "sr_pricing_refunded_cash"]),
            ["sd_where"])

        customer_total_return = ExecutionNode("customer_total_return",
            lambda sd_where: sd_where
                .groupby(["sr_customer_sk", "sr_store_sk"])
                .agg([pl.sum("sr_pricing_refunded_cash")
                .alias("ctr_total_return")]),
            ["sd_select"])

        ctr2 = ExecutionNode("ctr2",
            lambda sd_select: sd_select
                .groupby(["sr_store_sk"])
                .agg([pl.avg("sr_pricing_refunded_cash")
                .alias("ctr_avg_return")]),
            ["sd_select"])

        ctr_join = ExecutionNode("ctr_join",
            lambda ctr, ctr2: ctr.join(ctr2, on = "sr_store_sk"),
            ["customer_total_return", "ctr2"])

        ctr1 = ExecutionNode("ctr1",
            lambda ctr_join: ctr_join
                .filter(pl.col("ctr_total_return") >
                        pl.col("ctr_avg_return") * 1.2),
            ["ctr_join"])

        store_where = ExecutionNode("store_where",
            lambda store: store.filter(pl.col("w_store_address_state") == "TN"),
            ["store"])

        ctr1_store_join = ExecutionNode("ctr1_store_join",
            lambda ctr1, store_where: ctr1
                .join(store_where, left_on = "sr_store_sk",
                      right_on = "w_store_sk"),
            ["ctr1", "store_where"])

        result = ExecutionNode("result",
            lambda ctr1_store_join, customer: ctr1_store_join
                .join(customer, left_on = "sr_customer_sk",
                      right_on = "c_customer_sk"),
            ["ctr1_store_join", "customer"])

        result_select = ExecutionNode("result_select",
            lambda result: result.select("c_customer_id"),
            ["result"])

        result_ordered = ExecutionNode("result_ordered",
            lambda result_select: result_select.sort("c_customer_id"),
            ["result_select"])

        result_limit = ExecutionNode("result_limit",
            lambda result_ordered: result_ordered.limit(10),
            ["result_ordered"])

        query1_nodes = [store_returns, date, store, customer, sd_join,
            sd_select, sd_where, customer_total_return, ctr2, ctr_join, ctr1,
            store_where, ctr1_store_join, result, result_select, result_ordered,
            result_limit]

        return query1_nodes

    # Query 2----------------------------------------------------------------
    if query_num == 2:
        web_sales = ExecutionNode("web_sales", lambda: read_table("web_sales"), [])
        catalog_sales = ExecutionNode("catalog_sales",
                                      lambda: read_table("catalog_sales"), [])
        date = ExecutionNode("date", lambda: read_table("date"), [])

        web_select = ExecutionNode("web_select",
            lambda web_sales: web_sales
                .select([pl.col("ws_sold_date_sk").alias("sold_date_sk"),
                         pl.col("ws_pricing_ext_sales_price")
                         .alias("sales_price")]),
            ["web_sales"])

        catalog_select = ExecutionNode("catalog_select",
            lambda catalog_sales: catalog_sales
                .select([pl.col("cs_sold_date_sk").alias("sold_date_sk"),
                         pl.col("cs_pricing_ext_sales_price")
                         .alias("sales_price")]),
            ["catalog_sales"])

        web_catalog_join = ExecutionNode("web_catalog_join",
            lambda web_select, catalog_select: web_select
                                         .vstack(catalog_select),
            ["web_select", "catalog_select"])

        wscs = ExecutionNode("wscs",
            lambda web_catalog_join: web_catalog_join
                .select([pl.col("sold_date_sk"), pl.col("sales_price")]),
            ["web_catalog_join"])

        wscs_date_join = ExecutionNode("wscs_date_join",
            lambda wscs, date: wscs
                .join(date, left_on = "sold_date_sk", right_on = "d_date_sk"),
            ["wscs", "date"])

        wswscs = ExecutionNode("wswscs",
            lambda wscs_date_join: wscs_date_join
                .groupby(["d_week_seq"])
                .agg([pl.sum("sales_price").alias("sales_price")]),
            ["wscs_date_join"])

        date_filter_y = ExecutionNode("date_filter_y",
            lambda date: date.filter(pl.col("d_year") == 2000),
            ["date"])

        y = ExecutionNode("y",
            lambda wswscs, date_filter_y: wswscs
                .join(date_filter_y, on = "d_week_seq"),
            ["wswscs", "date_filter_y"])

        y_select = ExecutionNode("y_select",
            lambda y: y
                .select([pl.col("d_week_seq").alias("d_week_seq1"),
                         pl.col("sales_price").alias("sales_price1")]),
            ["y"])

        date_filter_z = ExecutionNode("date_filter_z",
            lambda date: date.filter(pl.col("d_year") == 2001),
            ["date"])

        z = ExecutionNode("z",
            lambda wswscs, date_filter_z: wswscs
                .join(date_filter_z, on = "d_week_seq"),
            ["wswscs", "date_filter_z"])

        z_select = ExecutionNode("z_select",
            lambda z: z
                .select([pl.col("d_week_seq").map(lambda x: x - 53)
                .alias("d_week_seq2"),
                         pl.col("sales_price").alias("sales_price2")]),
            ["z"])

        yz_join = ExecutionNode("yz_join",
            lambda y_select, z_select: y_select
                .join(z_select, left_on = "d_week_seq1",
                      right_on = "d_week_seq2"),
            ["y_select", "z_select"])

        yz_select = ExecutionNode("yz_select",
            lambda yz_join: yz_join
                .select([pl.col("d_week_seq1"),
                         pl.col("sales_price1") / pl.col("sales_price2")]),
            ["yz_join"])

        result = ExecutionNode("result",
            lambda yz_select: yz_select.sort("d_week_seq1"),
            ["yz_select"])

        query2_nodes = [web_sales, catalog_sales, date, web_select,
                        catalog_select, web_catalog_join, wscs, wscs_date_join,
                        wswscs,date_filter_y, y, y_select, date_filter_z, z,
                        z_select, yz_join, yz_select, result]

        return query2_nodes

    # Query 3----------------------------------------------------------------
    if query_num == 3:
        store_sales = ExecutionNode("store_sales",
                                    lambda: read_table("store_sales"), [])
        date = ExecutionNode("date", lambda: read_table("date"), [])
        item = ExecutionNode("item", lambda: read_table("item"), [])

        date_where = ExecutionNode("date_where",
            lambda date: date.filter(pl.col("d_moy") == 11),
            ["date"])

        item_where = ExecutionNode("item_where",
            lambda item: item.filter(pl.col("i_manufact_id") == 500),
            ["item"])

        date_ss_join = ExecutionNode("date_ss_join",
            lambda date, store_sales: date
                .join(store_sales, left_on = "d_date_sk",
                      right_on = "ss_sold_date_sk"),
            ["date_where", "store_sales"])

        all_join = ExecutionNode("all_join",
            lambda date_ss_join, item: date_ss_join
                .join(item, left_on = "ss_sold_item_sk",
                      right_on = "i_item_sk"),
            ["date_ss_join", "item_where"])

        groupby = ExecutionNode("groupby",
            lambda all_join: all_join
                .groupby(["d_year", "i_brand", "i_brand_id"])
                .agg([pl.sum("ss_pricing_net_profit")
                .alias("ss_pricing_net_profit")]),
            ["all_join"])

        orderby = ExecutionNode("orderby",
            lambda groupby: groupby
                .sort(["d_year", "ss_pricing_net_profit", "i_brand_id"],
                      reverse = [False, True, False]),
            ["groupby"])

        limit = ExecutionNode("limit",
            lambda orderby: orderby.limit(10),
            ["orderby"])

        query3_nodes = [store_sales, date, item, date_where, item_where,
                        date_ss_join, all_join, groupby, orderby, limit]

        return query3_nodes

    # Query 5----------------------------------------------------------------
    if query_num == 5:
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

    # Query 6----------------------------------------------------------------
    if query_num == 6:
        store_sales = ExecutionNode("store_sales",
                                    lambda: read_table("store_sales"), [])
        customer_address = ExecutionNode("customer_address",
                                    lambda: read_table("customer_address"), [])
        customer = ExecutionNode("customer",
                                 lambda: read_table("customer"), [])
        date = ExecutionNode("date", lambda: read_table("date"), [])
        item = ExecutionNode("item", lambda: read_table("item"), [])

        date_select = ExecutionNode("date_select",
            lambda date: date
                .filter((po.col("d_year") == 2000) & (po.col("d_moy") == 7)),
            ["date"])

        item2 = ExecutionNode("item2",
            lambda item: item
                .groupby("i_category")
                .agg(po.avg("i_current_price")
                .alias("i_current_price_avg")),
            ["item"])

        item_item2_join = ExecutionNode("item_item2_join",
            lambda item, item2: item.join(item2, on = "i_category"),
            ["item", "item2"])

        item_item2_filter = ExecutionNode("item_item2_filter",
            lambda item_item2_join: item_item2_join
                .filter(po.col("i_current_price") > 1.2 *
                        po.col("i_current_price_avg")),
            ["item_item2_join"])

        ac_join = ExecutionNode("ac_join",
            lambda customer_address, customer: customer_address
                .join(customer, left_on = "ca_address_sk",
                      right_on = "c_current_addr_sk"),
            ["customer_address", "customer"])

        acs_join = ExecutionNode("acs_join",
            lambda ac_join, store_sales: ac_join
                .join(store_sales, left_on = "c_customer_sk",
                      right_on = "ss_sold_customer_sk"),
            ["ac_join", "store_sales"])

        acsd_join = ExecutionNode("acsd_join",
            lambda acs_join, date_select: acs_join
                .join(date_select, left_on = "ss_sold_date_sk",
                      right_on = "d_date_sk"),
            ["acs_join", "date_select"])

        result_join = ExecutionNode("result_join",
            lambda acsd_join, item_item2_filter: acsd_join
                .join(item_item2_filter, left_on = "ss_sold_item_sk",
                      right_on = "i_item_sk"),
            ["acsd_join", "item_item2_filter"])

        result_gb = ExecutionNode("result_gb",
            lambda result_join: result_join
                .groupby("i_category")
                .agg(po.count("i_current_price").alias("count")),
            ["result_join"])

        result_having = ExecutionNode("result_having",
            lambda result_gb: result_gb.filter(po.col("count") >= 10),
            ["result_gb"])

        result = ExecutionNode("result",
            lambda result_having: result_having.limit(10),
            ["result_having"])

        query6_nodes = [store_sales, customer_address, customer, date, item,
                        date_select, item2, item_item2_join, item_item2_filter,
                        ac_join, acs_join, acsd_join, result_join, result_gb,
                        result_having, result]

        return query6_nodes

