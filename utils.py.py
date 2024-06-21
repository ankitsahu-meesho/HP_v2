# Databricks notebook source
def calculate_days_since_last_action(temp_df):
    temp_df = temp_df.withColumn('user_sscat_days_since_last_atc', F.datediff(F.lit(label_date), F.col('user_sscat_latest_atc_date')) )
    temp_df = temp_df.withColumn('user_sscat_days_since_last_order', F.datediff(F.lit(label_date), F.col('user_sscat_latest_order_date')) )
    temp_df = temp_df.withColumn('user_sscat_days_since_last_wishlist', F.datediff(F.lit(label_date), F.col('user_sscat_latest_wishlist_date')) )
    return temp_df

# COMMAND ----------

def rename_and_bucketize_user_static_feats(temp_df):
    temp_df = temp_df.withColumn("OD_stage_cat", when(temp_df["OD_stage"] == 0, "0")
                                            .when((temp_df["OD_stage"] >= 1) & (temp_df["OD_stage"] <= 3), "1-3")
                                            .when((temp_df["OD_stage"] >= 4) & (temp_df["OD_stage"] <= 10), "4-10")
                                            .otherwise("10+"))
    temp_df = temp_df.withColumnsRenamed({"gender":"user_gender", "region":"user_region", "install_source": "user_app_install_source", "manufacturer": "user_mobilebrand", "rc_flag": "user_rc_flag", "OD_stage_cat": "user_OD_stage_cat"})
    temp_df = temp_df.select(["user_id", "user_gender", "user_region", "user_app_install_source", "user_mobilebrand", "user_rc_flag", "user_OD_stage_cat"])
    #bucketization of demo values
    temp_df = temp_df.withColumn(
        "user_region",
        when(F.col('user_region').isin(region_buckets), F.col('user_region')).otherwise("missing"))

    temp_df = temp_df.withColumn(
        "user_mobilebrand",
        when(F.col('user_mobilebrand').isin(mobilebrand_buckets), F.col('user_mobilebrand')).otherwise("missing"))
    
    temp_df = temp_df.withColumn(
        "user_app_install_source",
        when(F.col('user_app_install_source').isin(installsource_buckets), F.col('user_app_install_source')).otherwise("missing"))
    return temp_df

# COMMAND ----------

def create_sscat_demo_ratios(temp_df, attribute):
    temp_df = temp_df.withColumn(f"sscat_{attribute}_cbyv_last_28_days", F.col("sum(clicks)")/F.col("sum(views)"))
    temp_df = temp_df.withColumn(f"sscat_{attribute}_obyr_last_28_days", F.col("sum(orders)")/F.col("count(DISTINCT user_id)"))
    temp_df = temp_df.select(["sscat_id", f"{attribute}" , f"sscat_{attribute}_cbyv_last_28_days", f"sscat_{attribute}_obyr_last_28_days"])
    return temp_df

# COMMAND ----------

def bucketize_sscat_region(temp_df):
    temp_df = temp_df.withColumn(
        "region",
        when(F.col('region').isin(region_buckets), F.col('region')).otherwise("missing"))
    return temp_df

# COMMAND ----------

print("utils file imported!!!")