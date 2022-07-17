from pyspark.mllib.clustering import StreamingKMeans
from pyspark.rdd import RDD
from pyspark import since


class OnlineKMeans(StreamingKMeans):
    """
    rewrite class for rdd-based data
    """

    @since("1.5.0")
    def trainOn(self, train_data: RDD) -> None:
        """
        Train the model on the rdd-based data
        :param train_data: the format of rdd must be below format
                (x1, x2, ..., x3) => each element must be a point
        :return:
        """
        self._validate(train_data)
        self._model.update(train_data, self._decayFactor, self._timeUnit)

    @since("1.5.0")
    def predictOn(self, rdd: RDD) -> RDD:
        """
        Make predictions on a rdd.
        :param rdd: the format of rdd must be below format
                (x1, x2, ..., x3) => each element must be a point
        :return:
        """
        self._validate(rdd)
        return rdd.map(lambda x: self._model.predict(x))

    @since("1.5.0")
    def predictOnValues(self, rdd: RDD) -> RDD:
        """
        Make predictions on a keyed rdd.
        Returns a transformed rdd object.
        :param rdd: rdd with below format
                    (key, value) => value = point = (x1, x2, ..., xn)
        :return: transformed rdd
        """
        self._validate(rdd)
        return dstream.mapValues(lambda x: self._model.predict(x))  # type: ignore[union-attr]

    def _validate(self, rdd) -> None:
        if self._model is None:
            raise ValueError(
                "Initial centers should be set either by setInitialCenters " "or setRandomCenters."
            )
        if not isinstance(rdd, RDD):
            raise TypeError(
                "Expected data to be of type RDD, " "got type %s" % type(rdd)
            )
