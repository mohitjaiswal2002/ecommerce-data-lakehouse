from utils.transforms import (
    add_product_derived_columns,
    add_user_derived_columns,
    keep_latest_snapshot,
)


def test_keep_latest_snapshot_keeps_newest_version_per_key(spark):
    df = spark.createDataFrame(
        [
            ("bronze/products_20260101_090000.json", 1, 10.0),
            ("bronze/products_20260301_090000.json", 1, 12.0),
            ("bronze/products_20260201_090000.json", 1, 11.0),
            ("bronze/products_20260101_090000.json", 2, 5.0),
        ],
        ["source_file", "id", "price"],
    )

    result = {row["id"]: row["price"] for row in keep_latest_snapshot(df).collect()}

    assert result == {1: 12.0, 2: 5.0}


def test_keep_latest_snapshot_drops_helper_columns(spark):
    df = spark.createDataFrame([("a.json", 1)], ["source_file", "id"])

    assert keep_latest_snapshot(df).columns == ["id"]


def test_add_product_derived_columns(spark):
    df = spark.createDataFrame(
        [
            (1, 100.0, 10.0, 0),
            (2, 50.0, 0.0, 5),
            (3, 19.99, 12.5, 10),
        ],
        ["product_id", "price", "discount_percentage", "stock"],
    )

    rows = {
        row["product_id"]: (row["discounted_price"], row["stock_status"])
        for row in add_product_derived_columns(df, low_stock_threshold=10).collect()
    }

    assert rows == {
        1: (90.0, "Out of Stock"),
        2: (50.0, "Low Stock"),
        3: (17.49, "In Stock"),  # threshold is exclusive: stock == 10 is in stock
    }


def test_add_user_derived_columns_age_group_boundaries(spark):
    ages = [17, 18, 29, 30, 44, 45, 59, 60]
    df = spark.createDataFrame(
        [(i, "Emily", "Johnson", age) for i, age in enumerate(ages)],
        ["user_id", "first_name", "last_name", "age"],
    )

    rows = add_user_derived_columns(df).orderBy("user_id").collect()

    assert [row["age_group"] for row in rows] == [
        "Under 18", "18-29", "18-29", "30-44", "30-44", "45-59", "45-59", "60+",
    ]
    assert rows[0]["full_name"] == "Emily Johnson"
