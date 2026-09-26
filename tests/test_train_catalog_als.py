import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession

from experiments.recommendation.train_catalog_als import precision_at_k


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    yield session
    session.stop()


class FakeModel:
    """Stands in for a fitted ALS model: recommendForUserSubset just
    returns a fixed top-k per user, in the same nested-struct shape
    real ALS output has, so precision_at_k's join/explode logic can be
    tested without actually training anything.
    """

    def __init__(self, spark, recs_by_user):
        self.spark = spark
        self.recs_by_user = recs_by_user

    def recommendForUserSubset(self, user_df, k):
        rows = [
            Row(
                user_id=user_id,
                recommendations=[
                    Row(product_id=pid, rating=float(len(recs) - i)) for i, pid in enumerate(recs[:k])
                ],
            )
            for user_id, recs in self.recs_by_user.items()
        ]
        return self.spark.createDataFrame(rows)


def test_precision_at_k_counts_hits_correctly(spark):
    train_df = spark.createDataFrame([(1, 10), (1, 11)], ["user_id", "product_id"])
    test_df = spark.createDataFrame([(1, 20), (1, 21)], ["user_id", "product_id"])
    # recommends 20 (a hit) and 30 (a miss) at k=2 -> precision 0.5
    model = FakeModel(spark, {1: [20, 30]})

    precision, n_users = precision_at_k(model, train_df, test_df, k=2)
    assert precision == 0.5
    assert n_users == 1


def test_precision_at_k_excludes_already_seen_training_items(spark):
    train_df = spark.createDataFrame([(1, 20)], ["user_id", "product_id"])
    test_df = spark.createDataFrame([(1, 21)], ["user_id", "product_id"])
    # 20 was already trained-on; only 21 should be eligible, and it's a hit
    model = FakeModel(spark, {1: [20, 21]})

    precision, n_users = precision_at_k(model, train_df, test_df, k=1)
    assert precision == 1.0
    assert n_users == 1


def test_precision_at_k_zero_when_no_hits(spark):
    train_df = spark.createDataFrame([(1, 10)], ["user_id", "product_id"])
    test_df = spark.createDataFrame([(1, 20)], ["user_id", "product_id"])
    model = FakeModel(spark, {1: [30, 31]})

    precision, n_users = precision_at_k(model, train_df, test_df, k=2)
    assert precision == 0.0
    assert n_users == 1
