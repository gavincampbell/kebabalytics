from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
from kebabalytics.scores import calculate_meaningless_kpis


class TestMeaninglessKpis:
    """Test cases for the meaningless KPI calculation function."""

    def test_perfect_score_scenario(self, spark: SparkSession):
        """Test scenario where all metrics hit targets exactly."""
        # Create test data that hits all targets perfectly
        # 250 orders, £13.5 avg, 10 min prep, 5.0 rating
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        # Create 250 orders with perfect metrics
        test_data = []
        for i in range(250):
            test_data.append((
                "SHOP001",
                "Perfect Kebab Shop",
                "Manchester", 
                f"ORDER-{i:03d}",
                datetime(2025, 7, 9, 12, 0, 0),
                13.5,  # Target avg order value
                10,    # Target prep time
                5      # Perfect rating
            ))
        
        df = spark.createDataFrame(test_data, schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        assert len(scores) == 1
        
        score_row = scores[0]
        assert score_row["total_orders"] == 250
        assert abs(score_row["avg_order_value"] - 13.5) < 0.01
        assert score_row["avg_prep_time"] == 10.0
        assert score_row["avg_rating"] == 5.0
        assert score_row["meaningless_kpi"] == 100.0

    def test_low_performance_scenario(self, spark: SparkSession):
        """Test scenario with poor performance across all metrics."""
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        # Create 50 orders (20% of target) with poor metrics
        test_data = []
        for i in range(50):
            test_data.append((
                "SHOP002",
                "Struggling Kebab Shop",
                "Birmingham",
                f"ORDER-{i:03d}",
                datetime(2025, 7, 9, 12, 0, 0),
                6.0,   # Low order value
                20,    # High prep time
                1      # Poor rating
            ))
        
        df = spark.createDataFrame(test_data, schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        assert len(scores) == 1
        
        score_row = scores[0]
        assert score_row["total_orders"] == 50
        assert score_row["meaningless_kpi"] < 50.0  # Should be low overall

    def test_high_volume_penalty(self, spark: SparkSession):
        """Test that excessive order volume gets penalized."""
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        # Create 400 orders (160% of target) - should be penalized
        test_data = []
        for i in range(400):
            test_data.append((
                "SHOP003",
                "Busy Kebab Shop",
                "London",
                f"ORDER-{i:03d}",
                datetime(2025, 7, 9, 12, 0, 0),
                13.5,  # Good order value
                10,    # Good prep time
                4      # Good rating
            ))
        
        df = spark.createDataFrame(test_data, schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        score_row = scores[0]
        
        # Should be penalized for too many orders
        # Volume score: 100 - ((400/250) - 1) * 25 = 100 - 0.6 * 25 = 85
        # Volume component: 85 * 0.3 = 25.5
        # Other components should be near perfect, so total should be < 100
        assert score_row["meaningless_kpi"] < 95.0

    def test_null_values_handling(self, spark: SparkSession):
        """Test handling of null prep times and ratings."""
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        test_data = [
            ("SHOP004", "Null Test Shop", "Manchester", "ORDER-001", 
             datetime(2025, 7, 9, 12, 0, 0), 13.5, None, None),
            ("SHOP004", "Null Test Shop", "Manchester", "ORDER-002", 
             datetime(2025, 7, 9, 12, 0, 0), 13.5, 10, 4),
        ]
        
        df = spark.createDataFrame(test_data, schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        assert len(scores) == 1
        
        score_row = scores[0]
        # Should handle nulls gracefully and still produce a score
        assert score_row["meaningless_kpi"] is not None
        assert 0 <= score_row["meaningless_kpi"] <= 100

    def test_multiple_shops_same_day(self, spark: SparkSession):
        """Test calculation for multiple shops on the same day."""
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        test_data = []
        
        # Shop 1: Good performance
        for i in range(250):
            test_data.append((
                "SHOP001", "Good Shop", "Manchester", f"ORDER1-{i:03d}",
                datetime(2025, 7, 9, 12, 0, 0), 15.0, 8, 5
            ))
        
        # Shop 2: Poor performance  
        for i in range(100):
            test_data.append((
                "SHOP002", "Poor Shop", "Birmingham", f"ORDER2-{i:03d}",
                datetime(2025, 7, 9, 12, 0, 0), 8.0, 18, 2
            ))
        
        df = spark.createDataFrame(test_data, schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        assert len(scores) == 2
        
        # Find scores for each shop
        shop1_score = next(s for s in scores if s["shop_id"] == "SHOP001")
        shop2_score = next(s for s in scores if s["shop_id"] == "SHOP002")
        
        assert shop1_score["meaningless_kpi"] > shop2_score["meaningless_kpi"]

    def test_edge_case_zero_orders(self, spark: SparkSession):
        """Test behavior with empty dataset."""
        schema = StructType([
            StructField("shop_id", StringType(), True),
            StructField("shop_name", StringType(), True),
            StructField("shop_city", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("prep_time_minutes", IntegerType(), True),
            StructField("customer_rating", IntegerType(), True)
        ])
        
        df = spark.createDataFrame([], schema)
        result = calculate_meaningless_kpis(df)
        
        scores = result.collect()
        assert len(scores) == 0