# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

def get_target_label_user_sscat(bucket_path, label_date):
    target_df = spark.sql(f"""select user_id, sscat_id , clicks, CASE WHEN clicks > 0 
            THEN 1
            ELSE 0  END  AS click_given_view_flag
            from ds_dbc_ofs.user_sscat__raw_agg  where process_date = '{label_date}' and views>0 """)
    target_df.write.mode("overwrite").parquet(bucket_path+"target_label_user_sscat_click_flag")

# COMMAND ----------

def get_sscat_visitors_orders_last_x_days(bucket_path, label_date):
    max_delta = max(aggregation_range)
    user_sscat_raw_agg = spark.sql(f'''select * from ds_dbc_ofs.user_sscat__raw_agg where process_date >= date_sub('{label_date}', '{max_delta}')  
                                   and process_date< '{label_date}' and CAST(user_id AS int) is not null''')

    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("label_date", F.lit(label_date))
    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("day_difference", F.datediff(F.col("label_date"), F.col("process_date")))

    attribute_last_x_days = []

    for days in aggregation_range:
        attribute_last_x_days.append(F.sum(F.when(F.col("day_difference").between(1,days), F.col("orders")).otherwise(0)).alias(f"orders_last_{days}_days"))
        attribute_last_x_days.append(F.countDistinct(F.when(F.col("day_difference").between(1,days), F.col("user_id")).otherwise(0)).alias(f"visitors_last_{days}_days"))
    result = user_sscat_raw_agg.groupBy("sscat_id").agg(*attribute_last_x_days)
    #result.display() 
    result.write.mode("overwrite").parquet(bucket_path+"sscat_visitors_orders_last_x_days")

# COMMAND ----------

def get_user_static_feats(bucket_path, user_ids):
    user_static_df = spark.sql(f'''
                                select user_id,lower(user__gender) as gender, lower(user__region) as region, 
                                lower(user__install_source) as install_source, lower(user__mobile_manufacturer) as manufacturer, lower(user__rc_flag) as rc_flag
                                from ds_dbc_ofs.user__properties
                                where process_date in (select max(process_date) from  ds_dbc_ofs.user__properties where process_date<'{label_date}' ) 
                                ''')

    df = user_ids.join(user_static_df, on = "user_id", how = "left")
    user_lifetime_orders = spark.sql(f"""
          select user_id, user__platform_orders_lifetime as OD_stage
          from ds_dbc_ofs.user__rollup_agg 
          where process_date = (date('{label_date}') - 1)
          """)
    df= df.join(user_lifetime_orders, on = "user_id", how= "left")
    print("user_profile- size -",df.count())
    
    df.write.mode("overwrite").parquet(bucket_path+"user_static_features")


# COMMAND ----------

def get_user_attribute_feats(bucket_path, user_ids, label_date):
    feats_to_take = ["user_id"]
    all_attribute_list = cvocw_attribute_list
    aggregation_range = user_rollup_table_aggregation_range
    user_attribute_df = spark.sql(f"""select * from ds_dbc_ofs.user__rollup_agg where
                                process_date == (select max(process_date) from ds_dbc_ofs.user__rollup_agg where process_date<'{label_date}' ) """)
    df = user_ids.join(user_attribute_df, on= "user_id", how = "left")
    for days in aggregation_range:
        df= df.withColumn(f"user_cbyv_last_{days}_days" ,col(f"user__platform_clicks_{days}_days")/ col(f"user__platform_views_{days}_days"))
        feats_to_take.append(f"user_cbyv_last_{days}_days")
        for rating_bucket in rating_buckets:
            df = df.withColumn(f"user_cbyv_{rating_bucket}_last_{days}_days", col(f"user__platform_{rating_bucket}_clicks_{days}_days") / col(f"user__platform_{rating_bucket}_views_{days}_days"))
            feats_to_take.append(f"user_cbyv_{rating_bucket}_last_{days}_days")
        for price_bucket in price_buckets:
            df = df.withColumn(f"user_cbyv_{price_bucket}_last_{days}_days", col(f"user__platform_{price_bucket}_clicks_{days}_days") / col(f"user__platform_{price_bucket}_views_{days}_days"))
            feats_to_take.append(f"user_cbyv_{price_bucket}_last_{days}_days")
    
    for days in aggregation_range:
        for attribute in all_attribute_list:
            feats_to_take.append(f'user__platform_{attribute}_{days}_days')
    
    df = df.select(*(feats_to_take))

    #app open freq last x days
    ao_freq_aggregation_days = aggregation_range
    app_open_frequency = spark.sql(f"""
            select cast(user_id as string) ,concat(year,'-',month,'-',day) as date, count(distinct time) as app_open
            from silver.mixpanel_android__app_open 
            where concat(year,'-',month,'-',day) >= date('{label_date}') - {max(ao_freq_aggregation_days)} and concat(year,'-',month,'-',day) <= date('{label_date}') - 1
            group by 1, 2
            """) \
                .withColumn("date_diff", F.date_diff(F.lit(label_date), "date")) \
                .groupBy("user_id") \
                .agg(*[F.sum(F.when(F.col("date_diff") <= days, F.col("app_open")).otherwise(0)).alias(f"user_app_open_last_{days}_days") for days in ao_freq_aggregation_days])
    df = df.join(app_open_frequency, on= "user_id", how= "left")
    
    # days since last app open 
    last_ao_num_days = spark.sql(f"""
          select cast(user_id as string) , max(concat(year,'-',month,'-',day)) as date
          from silver.mixpanel_android__app_open 
          where concat(year,'-',month,'-',day) >= date('{label_date}') - 60 and concat(year,'-',month,'-',day) <= date('{label_date}') - 1
          group by 1
          """) \
            .withColumn("user_last_ao_num_days", F.date_diff(F.lit(label_date), "date")) \
            .select("user_id", "user_last_ao_num_days")
    df = df.join(last_ao_num_days, on = "user_id", how = "left")


    # number of days since last order
    user_last_order = spark.sql(f"""
                select distinct cast(user_id as string), max(order_date) as date
                from gold.order_master_bi 
                where order_date < date('{label_date}')
                group by 1
            """) \
            .withColumn("user_last_order_num_days", F.date_diff(F.lit(label_date), "date")) \
            .select("user_id", "user_last_order_num_days")
    df = df.join(user_last_order, on= "user_id", how = "left")

    user_order = spark.sql(f"""select * from gold.order_master_bi where order_status in (4,6) and order_date<'{label_date}' """)
    user_order = user_order.join(user_ids, on= "user_id", how = "right")
    user_aov = user_order.groupBy(["user_id"]).agg(F.mean('price'))
    user_aov = user_aov.withColumnRenamed("avg(price)", "user_aov")
    df = df.join(user_aov, on = "user_id", how = "left")

    print("user_attribute- size -",df.count())
    df.write.mode("overwrite").parquet(bucket_path+"user_attribute_features")


# COMMAND ----------

def get_sscat_user_demo_attribute_wise_covr(label_date):
    #sscat user_profile feats - update frequency?
    #user_static_attributes = user_static_attributes
    max_short_term_day_delta = max(aggregation_range)
    user_sscat_raw = spark.sql(f''' select * from ds_dbc_ofs.user_sscat__raw_agg where process_date >= date_sub('{label_date}', '{max_short_term_day_delta}') and process_date<'{label_date}' and CAST(user_id AS int) is not null''')
    #user_sscat_raw = user_sscat_raw.join(user_ids, on= "user_id", how= "right")
    
    user_static = spark.read.parquet(bucket_path+"user_static_features")
    user_static = user_static.withColumn("OD_stage_cat", when(user_static["OD_stage"] == 0, "0")
                                           .when((user_static["OD_stage"] >= 1) & (user_static["OD_stage"] <= 3), "1-3")
                                           .when((user_static["OD_stage"] >= 4) & (user_static["OD_stage"] <= 10), "4-10")
                                           .otherwise("10+"))
    user_sscat_raw = user_sscat_raw.join(user_static, on= "user_id", how = "inner")
    for attribute in user_static_attributes:
        print(attribute)
        attribute_wise_sscat_cvor =  user_sscat_raw.groupBy(['sscat_id', attribute]).agg(F.sum('views'), F.sum('clicks'), F.sum('orders'), F.countDistinct('user_id'))
        attribute_wise_sscat_cvor.write.mode("overwrite").parquet(bucket_path+f"sscat_{attribute}_wise_cvor")

# COMMAND ----------

def get_user_sscat_roll_up_days_search(bucket_path, user_ids, label_date):
    user_ids = user_ids.cache()
    max_delta = max(aggregation_range)
    attribute_list = cv_attribute_list

    user_sscat_search = spark.sql(f''' select * from ds_dbc_ofs.user_sscat_realestate_feedsource__raw_agg where realestate="catalog_search_results" and process_date >= date_sub('{label_date}', '{max_delta}') and process_date<'{label_date}' and CAST(user_id AS int) is not null''')
    user_ids = user_ids.cache()
    user_sscat_raw_agg = user_sscat_search.join(F.broadcast(user_ids), on = "user_id", how= "right")

    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("label_date", F.lit(label_date))
    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("day_difference", F.datediff(col("label_date"), col("process_date")))

    attribute_last_x_days = []
    for attribute in attribute_list:
        print(attribute)
        for days in aggregation_range:
            attribute_last_x_days.append(sum(F.when(col("day_difference").between(1,days), col(f"{attribute}")).otherwise(0)).alias(f"user_sscat_search_{attribute}_last_{days}_days"))
    result = user_sscat_raw_agg.groupBy("user_id", "sscat_id").agg(*attribute_last_x_days)
    result.write.mode("overwrite").parquet(bucket_path+"user_sscat_search_views_clicks")

# COMMAND ----------

# sscat_%catalogs_attribute_feats
def get_sscat_catalog_rating_share(bucket_path):
    catalog_feats = spark.sql(f"""select * from gold.catalog_master_bi """)
    rating_count_less_than_3 = catalog_feats.filter(catalog_feats.rating < 3).groupBy("ss_cat_id").count().withColumnRenamed("count", "catalogs_with_rating_lt_3")

    rating_count_3_4 = catalog_feats.filter((catalog_feats.rating >= 3) & (catalog_feats.rating<4)).groupBy("ss_cat_id").count().withColumnRenamed("count", "catalogs_with_rating_bw_3_4")

    rating_count_greater_4 = catalog_feats.filter(catalog_feats.rating >= 4).groupBy("ss_cat_id").count().withColumnRenamed("count", "catalogs_with_rating_ge_4")

    total_catalogs = catalog_feats.groupBy("ss_cat_id").count().withColumnRenamed("count", "sscat_total_catalogs")
    avg_rating = catalog_feats.groupBy("ss_cat_id").agg(F.mean("rating")).withColumnRenamed("avg(rating)", "sscat_avg_rating")
    avg_price = catalog_feats.groupBy("ss_cat_id").agg(F.mean("max_product_price")).withColumnRenamed("avg(max_product_price)", "sscat_mean_price")
    quantile_price = catalog_feats.groupBy("ss_cat_id").agg(F.median("max_product_price").alias("sscat_median_price"), F.expr("percentile(max_product_price, 0.25)").alias("sscat_25th_quant_price"), F.expr("percentile(max_product_price, 0.75)").alias("sscat_75th_quant_price") )

    sscat_wise_rating_share = (rating_count_less_than_3.join(rating_count_3_4, on = "ss_cat_id", how= "outer")).join(rating_count_greater_4, on = "ss_cat_id", how= "outer")
    sscat_wise_rating_share = sscat_wise_rating_share.join(total_catalogs, on = "ss_cat_id", how= "outer")
    sscat_wise_rating_share = sscat_wise_rating_share.join(avg_rating, on= "ss_cat_id", how = "outer")
    sscat_wise_rating_share = sscat_wise_rating_share.join(avg_price, on= "ss_cat_id", how = "outer")
    sscat_wise_rating_share = sscat_wise_rating_share.join(quantile_price, on= "ss_cat_id", how = "outer")

    sscat_wise_rating_share = sscat_wise_rating_share.withColumn("rating_lt_3_catalog_share" , F.col("catalogs_with_rating_lt_3")/F.col("sscat_total_catalogs"))
    sscat_wise_rating_share = sscat_wise_rating_share.withColumn("rating_3_4_catalog_share" , F.col("catalogs_with_rating_bw_3_4")/F.col("sscat_total_catalogs"))
    sscat_wise_rating_share = sscat_wise_rating_share.withColumn("rating_ge_4_catalog_share" , F.col("catalogs_with_rating_ge_4")/F.col("sscat_total_catalogs"))
    
    sscat_wise_rating_share = sscat_wise_rating_share.select(*["ss_cat_id", "rating_lt_3_catalog_share", "rating_3_4_catalog_share", "rating_ge_4_catalog_share", "sscat_avg_rating", "sscat_mean_price", "sscat_median_price", "sscat_25th_quant_price", "sscat_75th_quant_price"])
    sscat_wise_rating_share = sscat_wise_rating_share.withColumnRenamed("ss_cat_id", "sscat_id")
    sscat_wise_rating_share.write.mode("overwrite").parquet(bucket_path+"sscat_catalog_rating_price_share")


# COMMAND ----------

def get_user_sscat_rating_price_bucket_last_x_days(bucket_path, user_ids, label_date):
    max_delta = max(aggregation_range)
    interaction_df = spark.sql(f'''select * from ds_dbc_ofs.user_catalog__raw_agg where process_date >= date_sub('{label_date}', '{max_delta}') and process_date<'{label_date}' and CAST(user_id AS int) is not null and orders>0''')
    interaction_df = interaction_df.join(user_ids, on = "user_id", how = "right")
    catalog_info = spark.sql(f"""select * from gold.catalog_master_bi""")
    interaction_catalog_df = interaction_df.join(catalog_info, on = "catalog_id", how = "left")
    interaction_catalog_df = interaction_catalog_df.withColumnRenamed("ss_cat_id", "sscat_id")
    interaction_catalog_df = interaction_catalog_df.withColumn("label_date", F.lit(label_date))
    interaction_catalog_df = interaction_catalog_df.withColumn("day_difference", F.datediff(col("label_date"), col("process_date")))

    attribute_last_x_days = []
    for days in aggregation_range:
        attribute_last_x_days.append(sum(when(col("rating").between(0,2.99) & col("day_difference").between(1,days) , col(f"views") ).otherwise(0)).alias(f"rating_lt_3_views_{days}_days") ) 
        attribute_last_x_days.append(sum(when(col("rating").between(3,3.99) & col("day_difference").between(1,days), col(f"views") ).otherwise(0)).alias(f"rating_3_4_views_{days}_days") )
        attribute_last_x_days.append(sum(when(col("rating").between(4,5) & col("day_difference").between(1,days), col(f"views") ).otherwise(0)).alias(f"rating_ge_4_views_{days}_days") )

        attribute_last_x_days.append(sum(when(col("rating").between(0,2.99) & col("day_difference").between(1,days), col(f"clicks") ).otherwise(0)).alias(f"rating_lt_3_clicks_{days}_days") )
        attribute_last_x_days.append(sum(when(col("rating").between(3,3.99) & col("day_difference").between(1,days), col(f"clicks") ).otherwise(0)).alias(f"rating_3_4_clicks_{days}_days") )
        attribute_last_x_days.append(sum(when(col("rating").between(4,5) & col("day_difference").between(1,days), col(f"clicks") ).otherwise(0)).alias(f"rating_ge_4_clicks_{days}_days") )

        attribute_last_x_days.append(F.mean(when(col("day_difference").between(1, days) & (col("clicks")>0) , col(f'max_product_price') ).otherwise(0).alias(f'user_sscat_avg_click_price_last_{days}_days') ) )

    #price low, medium, high - clicks, views at userXsscat
    sscat_df = spark.read.parquet(bucket_path+"sscat_catalog_rating_price_share")
    interaction_catalog_df = interaction_catalog_df.join(sscat_df, on ="sscat_id", how = "left")
    interaction_catalog_df = interaction_catalog_df.withColumn("max_product_price_cat", 
                                                           F.when(F.col("max_product_price") < F.col("sscat_25th_quant_price"), "low_price_wrt_sscat")
                                                           .when((F.col("max_product_price") >= F.col("sscat_25th_quant_price")) & (F.col("max_product_price") <= F.col("sscat_75th_quant_price")), "medium_price_wrt_sscat")
                                                           .when(F.col("max_product_price") > F.col("sscat_75th_quant_price"), "high_price_wrt_sscat")
                                                           .otherwise("Unknown"))


    for days in aggregation_range:
        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="low_price_wrt_sscat", col(f"clicks") ).otherwise(0).alias(f'user_sscat_low_price_clicks_last_{days}_days') ) )
        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="medium_price_wrt_sscat", col(f"clicks") ).otherwise(0).alias(f'user_sscat_medium_price_clicks_last_{days}_days') ) )
        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="high_price_wrt_sscat", col(f"clicks") ).otherwise(0).alias(f'user_sscat_high_price_clicks_last_{days}_days') ) )

        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="low_price_wrt_sscat", col(f"views") ).otherwise(0).alias(f'user_sscat_low_price_views_last_{days}_days') ) )
        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="medium_price_wrt_sscat", col(f"views") ).otherwise(0).alias(f'user_sscat_medium_price_views_last_{days}_days') ) )
        attribute_last_x_days.append( F.sum(F.when(col("max_product_price_cat")=="high_price_wrt_sscat", col(f"views") ).otherwise(0).alias(f'user_sscat_high_price_views_last_{days}_days') ) )

    


    result = interaction_catalog_df.groupBy("user_id", "sscat_id").agg(*attribute_last_x_days)
    result.write.mode("overwrite").parquet(bucket_path+"user_sscat_rating_price_%_last_x_days")

# COMMAND ----------

def get_sscat_feats(bucket_path, label_date):
    sscat_feats = spark.read.table("ds_dbc_ofs.sscat__rollup_agg") \
                        .filter(F.col("process_date") == (F.to_date(F.lit(label_date))-1)) \
                        .select(*['sscat_id'] + [(F.col(f"sscat__platform_clicks_{days}_days")/F.col(f"sscat__platform_views_{days}_days")).alias(f"sscat__platform_cbyv_{days}_days") for days in aggregation_range] + [F.col(f"sscat__platform_clicks_{days}_days") for days in aggregation_range])
    #sscat_feats = sscat_feats.join(sscat_ids, on= "sscat_id", how= "right")
    sscat_feats.write.mode("overwrite").parquet(bucket_path+"sscat_attribute_feats")
    

# COMMAND ----------

def get_user_sscat_roll_up_days(bucket_path, user_ids, label_date):
    user_ids = user_ids.cache()
    max_short_term_day_delta = max(aggregation_range)+1
    attribute_list = cvocw_attribute_list

    user_sscat_raw_agg = spark.sql(f'''select * from ds_dbc_ofs.user_sscat__raw_agg  where process_date >= date_sub('{label_date}', '{max_short_term_day_delta}') and process_date<'{label_date}' and CAST(user_id AS int) is not null''')
    user_sscat_raw_agg = user_sscat_raw_agg.join(user_ids, on = "user_id", how = "right")

    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("label_date", F.lit(label_date))
    user_sscat_raw_agg = user_sscat_raw_agg.withColumn("day_difference", F.datediff(col("label_date"), col("process_date")))

    attribute_last_x_days = []
    for attribute in attribute_list:
        print(attribute)
        for days in aggregation_range:
            attribute_last_x_days.append(sum(when(col("day_difference").between(1,days), col(f"{attribute}")).otherwise(0)).alias(f"user_sscat_{attribute}_last_{days}_days"))
    result = user_sscat_raw_agg.groupBy("user_id", "sscat_id").agg(*attribute_last_x_days)

    user_sscat_order = spark.sql(f"""select * from gold.order_master_bi  where order_status in (4,6) and order_date<'{label_date}' """)
    user_sscat_order = user_sscat_order.join(user_ids, on = "user_id", how = "right")
    user_sscat_aov = user_sscat_order.groupBy(["user_id", "sscat_id"]).agg(F.mean('price'))
    user_sscat_aov = user_sscat_aov.withColumnRenamed("avg(price)", "user_sscat_aov")
    result = result.join(user_sscat_aov, on= ["user_id", "sscat_id"], how = "outer")

    user_sscat_order_rating = user_sscat_order.filter(user_sscat_order.order_rating.isNotNull())
    user_sscat_order_rating = user_sscat_order_rating.groupBy(["user_id", "sscat_id"]).agg(F.mean("order_rating"))
    user_sscat_order_rating = user_sscat_order_rating.withColumnRenamed("avg(order_rating)", "user_sscat_order_rating")
    result = result.join(user_sscat_order_rating, on = ["user_id", "sscat_id"], how= "left")

    user_sscat_order_share = user_sscat_order.groupBy("user_id", "sscat_id").agg(F.countDistinct("order_id").alias("user_sscat_order_count"))
    user_order_share = user_sscat_order_share.groupBy("user_id").agg(F.sum("user_sscat_order_count").alias("user_total_orders"))
    user_sscat_order_ratio = user_sscat_order_share.join(user_order_share, on="user_id", how="inner")
    user_sscat_order_ratio = user_sscat_order_ratio.withColumn("user_sscat_order_share", F.col("user_sscat_order_count") / F.col("user_total_orders"))
    result = result.join(user_sscat_order_ratio, on = ["user_id", "sscat_id"], how = "left")

    user_sscat_latest_atc_dates = user_sscat_raw_agg.filter("carts > 0").groupBy("user_id", "sscat_id").agg(F.max("process_date").alias("user_sscat_latest_atc_date"))
    user_sscat_latest_wishlist_dates = user_sscat_raw_agg.filter("wishlists > 0").groupBy("user_id", "sscat_id").agg(F.max("process_date").alias("user_sscat_latest_wishlist_date"))
    user_sscat_latest_order_dates = user_sscat_raw_agg.filter("orders > 0").groupBy("user_id", "sscat_id").agg(F.max("process_date").alias("user_sscat_latest_order_date"))
    user_sscat_ocw_latest_dates = (user_sscat_latest_atc_dates.join(user_sscat_latest_order_dates, on = ["user_id", "sscat_id"], how = "outer")).join(user_sscat_latest_wishlist_dates, on = ["user_id", "sscat_id"], how = "outer")
    result = result.join(user_sscat_ocw_latest_dates, on =["user_id", "sscat_id"], how = "left")

    result.write.mode("overwrite").parquet(bucket_path+"user_sscat_agg_feats")


# COMMAND ----------


# # not using this, not scalable, instead refer to get_user_sscat_roll_up_days
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum, when, date_sub, current_date

# def get_user_sscat_short_term_interaction_feats(bucket_path, user_ids, label_date):

#     # Read the user_catalog__raw_agg table
#     max_short_term_day_delta = all_aggregation_range
#     user_catalog_raw_agg = spark.sql(f'''select * from ds_dbc_ofs.user_catalog__raw_agg where process_date >= date_sub('{label_date}', '{max_short_term_day_delta}') and process_date<'{label_date}'  and CAST(user_id AS int) is not null''')
#     catalog_sscat_map = spark.sql('''select distinct(catalog_id), sscat, sscat_id from gold.product_info''')

#     print("size", user_catalog_raw_agg.count())
#     user_catalog_raw_agg = user_catalog_raw_agg.join(user_ids, on= "user_id", how= "right")
#     print("size", user_catalog_raw_agg.count())
#     user_catalog_raw_agg = user_catalog_raw_agg.join(catalog_sscat_map, on= "catalog_id", how = "left")

#     # Calculate the last 3 days' clicks
#     attribute_last_x_days = []
#     attribute_list = ["views", "clicks"]
#     for days in aggregation_range:
#         for attribute in attribute_list:
#             attribute_last_x_days.append( sum(when(col("process_date") == date_sub(current_date(), days), col(f"{attribute}")).otherwise(0)).alias(f"user_sscat_{attribute}_last_{days}_day") )


#     clicks_last_1_day = sum(when(col("process_date") == date_sub(current_date(), 1), col("clicks")).otherwise(0)).alias("clicks_last_1_day")
#     clicks_last_2_days = sum(when(col("process_date") == date_sub(current_date(), 2), col("clicks")).otherwise(0)).alias("clicks_last_2_days")
#     clicks_last_3_days = sum(when(col("process_date") == date_sub(current_date(), 3), col("clicks")).otherwise(0)).alias("clicks_last_3_days")

#     views_last_1_day = sum(when(col("process_date") == date_sub(current_date(), 1), col("views")).otherwise(0)).alias("views_last_1_day")
#     views_last_2_days = sum(when(col("process_date") == date_sub(current_date(), 2), col("views")).otherwise(0)).alias("views_last_2_days")
#     views_last_3_days = sum(when(col("process_date") == date_sub(current_date(), 3), col("views")).otherwise(0)).alias("views_last_3_days")

#     # Group by user_id and catalog_id and calculate the sum of clicks for the last 3 days
#     result = user_catalog_raw_agg.groupBy("user_id", "sscat_id", "sscat").agg(clicks_last_1_day, clicks_last_2_days, clicks_last_3_days, views_last_1_day, views_last_2_days, views_last_3_days )

#     user_sscat_short_term_interactions = result.select(*( ['user_id','sscat', 'sscat_id'] + [(col(f'{interaction}_last_1_day') + col(f'{interaction}_last_2_days') + col(f'{interaction}_last_3_days') ).alias(f'user_sscat_last_3_days_{interaction}') for interaction in ["views", "clicks"]] + [ (col(f'{interaction}_last_1_day') ).alias(f'user_sscat_last_1_days_{interaction}') for interaction in ['views', 'clicks'] ] ))

#     print("final size of df", user_sscat_short_term_interactions.count())
#     user_sscat_short_term_interactions.write.mode("overwrite").parquet(bucket_path+"user_short_term_interaction_features")

# COMMAND ----------

print('feature generator file imported!!!')