"""Fit a tabular rating model and a training-mean reference."""

from __future__ import annotations

from typing import Any, NamedTuple

from ecommerce_analytics.rating_matrix import RatingMatrix


class RatingPredictions(NamedTuple):
    baseline: Any
    model: Any
    paired: Any
    estimator: Any


def fit_rating_model(matrix: RatingMatrix) -> RatingPredictions:
    """Fit a random forest on train rows and predict the held-out test rows."""
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.sql import functions as F

    if not matrix.features:
        raise ValueError("rating matrix has no model features")

    assembler = VectorAssembler(
        inputCols=list(matrix.features),
        outputCol="features",
        handleInvalid="error",
    )
    train = assembler.transform(matrix.train)
    test = assembler.transform(matrix.test)
    estimator = RandomForestRegressor(
        featuresCol="features",
        labelCol="rating",
        predictionCol="prediction",
        numTrees=30,
        maxDepth=6,
        seed=42,
    )
    model = estimator.fit(train)
    predicted = model.transform(test)
    predictions = predicted.select("rating", "review_month", "prediction")
    baseline = matrix.test.select("rating", "review_month").withColumn(
        "prediction", F.lit(matrix.baseline_mean)
    )
    paired = predicted.select(
        "rating",
        "review_month",
        F.lit(matrix.baseline_mean).alias("baseline_prediction"),
        F.col("prediction").alias("model_prediction"),
    )
    return RatingPredictions(
        baseline=baseline,
        model=predictions,
        paired=paired,
        estimator=model,
    )
