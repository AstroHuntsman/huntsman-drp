from huntsman.drp.core import get_logger


class MetricEvaluator():

    def __init__(self):
        self.functions = set()
        self.logger = get_logger()

    def add_function(self, func):
        """ Function decorator method to add function to list of metrics.
        Args:
            func (Function): The function to add.
        Returns:
            func (Function): The input function.
        """
        self.functions.add(func)
        return func

    def evaluate(self, to_skip=None, *args, **kwargs):
        """ Evaluate metrics by calling all funcions with common arguments.
        Args:
            to_skip (iterable of str, optional): If provided, skip these functions when
                evaluating metrics.
            *args, **kwargs: Parsed to metric function calls.
        Returns:
            dict: The evaluated metrics.
        """
        results = {}
        success = True
        if to_skip is None:
            to_skip = []

        for func in self.functions:
            if func in to_skip:
                self.logger.debug(f"Skipping metric: {func.__name__}")
                continue
            try:
                results.update(func(*args, **kwargs))
            except Exception as err:
                self.logger.warning(f"Exception while evaluating metric {func.__name__}: {err!r}")
                success = False

        return results, success
