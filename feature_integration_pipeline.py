# Databricks notebook source
from pyhocon import ConfigFactory
from datetime import datetime
from pyspark.sql.functions import col, sum, when, date_sub, current_date

# COMMAND ----------

# MAGIC %run ./feat_generator.py

# COMMAND ----------

# MAGIC %run ./utils.py

# COMMAND ----------

today_date = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

conf_file = "./sscat_ranking.config"
app_config = ConfigFactory.parse_file(conf_file)

label_date = app_config.get_string("label_date")
bucket_path = app_config.get_string("bucket_path") + label_date + "/"
price_buckets = app_config.get_list("price_buckets")
rating_buckets = app_config.get_list("rating_buckets")
cvocw_attribute_list = app_config.get_list("cvocw_attribute_list")
cv_attribute_list = app_config.get_list("cv_attribute_list")
user_static_attributes = app_config.get_list("user_static_attributes")
aggregation_range = app_config.get_list("aggregation_range")
user_rollup_table_aggregation_range = app_config.get_list("user_rollup_table_aggregation_range")
region_buckets = app_config.get_list("region_buckets")
mobilebrand_buckets = app_config.get_list("mobilebrand_buckets")
installsource_buckets = app_config.get_list("installsource_buckets")


# COMMAND ----------

bucket_path

# COMMAND ----------

get_target_label_user_sscat(bucket_path, label_date)

# COMMAND ----------

#for user_training user base sampling - sample size = 2M
#from pyspark.sql.types import IntegerType

#user_ids = spark.read.parquet(bucket_path+"target_label_user_sscat_click_flag").select("user_id").distinct()

#user_ids = user_ids.filter(user_ids.user_id.cast(IntegerType()).isNotNull())
#interactions = spark.read.parquet(bucket_path+"target_label_user_sscat_click_flag").select("user_id")
#interactions.count()
#user_base_size = 2000000
#ratio = user_base_size/user_ids.count()
#user_ids = user_ids.sample(False, ratio)
#user_ids.count()
#user_ids.write.mode("overwrite").parquet(bucket_path+"sampled_training_user_base")

# COMMAND ----------

user_ids = spark.read.parquet(bucket_path+"sampled_training_user_base")

# COMMAND ----------

user_ids.count()

# COMMAND ----------

get_sscat_visitors_orders_last_x_days(bucket_path, label_date)

# COMMAND ----------

get_user_static_feats(bucket_path, user_ids)

# COMMAND ----------

get_user_attribute_feats(bucket_path, user_ids, label_date)

# COMMAND ----------

get_sscat_user_demo_attribute_wise_covr(label_date) # this needs o/p of get_user_static_feats

# COMMAND ----------

get_user_sscat_roll_up_days_search(bucket_path, user_ids, label_date)

# COMMAND ----------

get_sscat_catalog_rating_share(bucket_path)

# COMMAND ----------

get_sscat_feats(bucket_path, label_date)

# COMMAND ----------

get_user_sscat_roll_up_days(bucket_path, user_ids, label_date)

# COMMAND ----------

get_user_sscat_rating_price_bucket_last_x_days(bucket_path, user_ids, label_date) #this needs o/p of get_sscat_catalog_rating_share

# COMMAND ----------

sscat_files = ["sscat_attribute_feats", "sscat_catalog_rating_price_share", "sscat_visitors_orders_last_x_days"]

user_files = ["user_attribute_features", "user_static_features"]

user_sscat_files = ["user_sscat_agg_feats", "user_sscat_rating_price_%_last_x_days", "user_sscat_search_views_clicks"] 

#append these files at the very last with join on user_id, sscat_id and demo_attribute
sscat_user_demo_files = ["sscat_OD_stage_cat_wise_cvor", "sscat_gender_wise_cvor", "sscat_region_wise_cvor"]

target = "target_label_user_sscat_click_flag"
user_ids_file = "sampled_training_user_base"

# COMMAND ----------

user_sscat_agg_df = calculate_days_since_last_action(spark.read.parquet(bucket_path+"user_sscat_agg_feats"))
user_sscat_agg_df.display()

# COMMAND ----------

user_sscat_features = user_sscat_agg_df.join(spark.read.parquet(bucket_path+"user_sscat_rating_price_%_last_x_days"), on = ["user_id", "sscat_id"], how = "outer")
user_sscat_features = user_sscat_features.join(spark.read.parquet(bucket_path+ "user_sscat_search_views_clicks"), on = ["user_id", "sscat_id"], how = "outer")

# COMMAND ----------

user_sscat_files

# COMMAND ----------

#rename user_attribute file columns
user_static_df = rename_and_bucketize_user_static_feats(spark.read.parquet(bucket_path+"user_static_features"))
user_static_df.display()

# COMMAND ----------

user_features = user_static_df.join(spark.read.parquet(bucket_path+"user_attribute_features"), on = "user_id", how = "outer")
user_features.display()

# COMMAND ----------

user_files

# COMMAND ----------

# compute sscat attribute ratios
sscat_attribute_df = spark.read.parquet(bucket_path+"sscat_visitors_orders_last_x_days")
for days in aggregation_range:
    sscat_attribute_df = sscat_attribute_df.withColumn(f'sscat_obyr_last_{days}_days', F.col(f'orders_last_{days}_days')/F.col(f'visitors_last_{days}_days') )

# COMMAND ----------

sscat_features = sscat_attribute_df.join(spark.read.parquet(bucket_path+"sscat_catalog_rating_price_share"), on="sscat_id", how = "outer")
sscat_features = sscat_features.join(spark.read.parquet(bucket_path+ "sscat_attribute_feats"), on = "sscat_id", how = "outer")
sscat_features.display()

# COMMAND ----------

sscat_files

# COMMAND ----------

#merge user_sscat_features and user_features 
user_sscat_features = user_sscat_features.join(user_features, on= "user_id", how = "left")
user_sscat_features.display()

# COMMAND ----------

#merge user_sscat_features and sscat_features
user_sscat_features = user_sscat_features.join(sscat_features, on = "sscat_id", how = "left")
user_sscat_features.display()

# COMMAND ----------

#computing sscat demo wise ratios
sscat_region_wise_cvor = create_sscat_demo_ratios(bucketize_sscat_region(spark.read.parquet(bucket_path+"sscat_region_wise_cvor")), "region")
sscat_gender_wise_cvor = create_sscat_demo_ratios(spark.read.parquet(bucket_path+f"sscat_gender_wise_cvor"), "gender")
sscat_OD_stage_cat_wise_cvor = create_sscat_demo_ratios(spark.read.parquet(bucket_path+f"sscat_OD_stage_cat_wise_cvor"), "OD_stage_cat")



# COMMAND ----------

sscat_gender_wise_cvor.display()

# COMMAND ----------

# merge user_sscat_features and sscat_user_demo_features
user_sscat_features = user_sscat_features.join(
    sscat_gender_wise_cvor,
    on=[
        user_sscat_features["sscat_id"] == sscat_gender_wise_cvor["sscat_id"],
        user_sscat_features["user_gender"] == sscat_gender_wise_cvor["gender"]
    ],
    how="left"
).drop(sscat_gender_wise_cvor.sscat_id)


user_sscat_features = user_sscat_features.join(
    sscat_region_wise_cvor,
    on=[
        user_sscat_features["sscat_id"] == sscat_region_wise_cvor["sscat_id"],
        user_sscat_features["user_region"] == sscat_region_wise_cvor["region"]
    ],
    how="left"
).drop(sscat_region_wise_cvor.sscat_id)

user_sscat_features = user_sscat_features.join(
    sscat_OD_stage_cat_wise_cvor,
    on=[
        user_sscat_features["sscat_id"] == sscat_OD_stage_cat_wise_cvor["sscat_id"],
        user_sscat_features["user_OD_stage_cat"] == sscat_OD_stage_cat_wise_cvor["OD_stage_cat"]
    ],
    how="left"
).drop(sscat_OD_stage_cat_wise_cvor.sscat_id)
user_sscat_features.display()

# COMMAND ----------

# target labels for training user base
user_sscat_target_labels = spark.read.parquet(bucket_path+"target_label_user_sscat_click_flag")
user_sscat_target_labels.display()

# COMMAND ----------

user_sscat_target_labels = user_sscat_target_labels.join(user_ids, on="user_id", how = "right")
user_sscat_target_labels.count()

# COMMAND ----------

user_sscat_target_labels.display()

# COMMAND ----------

user_sscat_features.count()

# COMMAND ----------

user_ids.count()

# COMMAND ----------

total_interactions = spark.sql('''select * from ds_dbc_ofs.user_sscat__raw_agg  where process_date = '2024-06-10' ''')
total_interactions.count()

# COMMAND ----------

total_interactions.select('user_id').distinct().count()