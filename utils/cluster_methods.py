from pyspark.mllib.clustering import StreamingKMeans
from pyspark.ml.clustering import KMeans
from pyspark.sql.dataframe import DataFrame
from pyspark import since


class StructuredStreamingKMeans(StreamingKMeans):

    @since("1.5.0")
    def trainOn(self, train_dataframe: DataFrame) -> None:
        """Train the model on the incoming streaming dataframe."""
        self._validate(train_dataframe)

        def update(x) -> None:
            rdd = x.rdd
            self._model.update(rdd, self._decayFactor, self._timeUnit)  # type: ignore[union-attr]

        train_dataframe.writeStream.foreach(update).start()

    def _predict_rdd_string_format(self, x):
        rdd = x.rdd
        result = self._model.predict(rdd)
        return result.zipWithIndex()

    @since("1.5.0")
    def predictOn(self, dataframe: DataFrame, train_columns: list):
        """
        Make predictions on a dataframe.
        Returns a dataframe with cluster column object
        """
        self._validate(dataframe)
        return dataframe.writeStream.foreach(lambda x: x.withColumn("Cluster",
                                                                    self._model.predict(x.select(train_columns).rdd)))
        #return dstream.map(lambda x: self._model.predict(x))  # type: ignore[union-attr]

    @since("1.5.0")
    def predictOnValues(self, dstream: "DStream[Tuple[T, VectorLike]]") -> "DStream[Tuple[T, int]]":
        """
        Make predictions on a keyed dstream.
        Returns a transformed dstream object.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))  # type: ignore[union-attr]

    def _validate(self, dataframe) -> None:
        if self._model is None:
            raise ValueError(
                "Initial centers should be set either by setInitialCenters " "or setRandomCenters."
            )
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "Expected dataframe to be of type DataFrame, " "got type %s" % type(dataframe)
            )
