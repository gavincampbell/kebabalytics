from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_meaningless_kpis(df: DataFrame) -> DataFrame:
    """
    
    Score based on:
    - Order volume vs. target (250 orders/day) - 30% weight
    - Average order value vs. target (£13.5) - 30% weight  
    - Prep time vs. target (10 minutes) - 20% weight
    - Customer rating (1-5 scale) - 20% weight
    """
    # Define targets
    target_orders = 250
    target_value = 13.5
    target_prep = 10


    daily_totals = df.groupBy(
        "shop_id", "shop_name", "shop_city", F.to_date("order_timestamp").alias("order_date")
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.avg("prep_time_minutes").alias("avg_prep_time"),
        F.avg("customer_rating").alias("avg_rating"),

    )
    
    # Calculate meaningless KPI with all components
    return daily_totals.withColumn(
        "meaningless_kpi",
        F.round(
            # Order volume score (30% weight) - peaks at target, gentler penalties for excess
            (F.when(
                (F.col("total_orders") / target_orders) <= 1, 
                (F.col("total_orders") / target_orders) * 100
            ).otherwise(
                F.greatest(F.lit(0), 100 - ((F.col("total_orders") / target_orders) - 1) * 25)
            ) * 0.3) +
            
            # Average order value score (30% weight) - rewards up to 2x target
            (F.when(
                F.col("avg_order_value") <= target_value,
                (F.col("avg_order_value") / target_value) * 100
            ).otherwise(
                F.least(
                    F.lit(100),
                    100 + ((F.col("avg_order_value") - target_value) / target_value) * 50
                )
            ) * 0.3) +
            
            # Prep time score (20% weight) - lower is better
            (F.when(
                F.col("avg_prep_time") <= target_prep,
                F.lit(100)
            ).otherwise(
                F.greatest(F.lit(0), 100 - (F.col("avg_prep_time") - target_prep) * 5)
            ) * 0.2) +
            
            # Customer rating score (20% weight)
            (((F.coalesce(F.col("avg_rating"), F.lit(3.0)) / 5.0) * 100) * 0.2),
            
            1
        )
    )