"""Fit a tabular rating model and a training-mean reference."""

from __future__ import annotations

from typing import Any, NamedTuple

from ecommerce_analytics.rating_matrix import RatingMatrix


class RatingPredictions(NamedTuple):
    baseline: Any
    model: Any
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
    predictions = model.transform(test).select("rating", "prediction")
    baseline = matrix.test.select("rating").withColumn(
        "prediction", F.lit(matrix.baseline_mean)
    )
    return RatingPredictions(baseline=baseline, model=predictions, estimator=model)
