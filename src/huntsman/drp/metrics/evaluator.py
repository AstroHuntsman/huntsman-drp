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
        self.logger.debug(f"Adding function to {self}: {func.__name__}")
        self.functions.add(func)
        return func

    def remove_function(self, function_name):
        """ Remove a function from the evaluator.
        Args:
            function_name (str): The name of the function to remove.
        """
        self.logger.debug(f"Removing function from {self}: {function_name}")
        self.functions = set([f for f in self.functions if f.__name__ != function_name])

    def evaluate(self, *args, **kwargs):
        """ Evaluate metrics by calling all funcions with common arguments.
        Args:
            *args, **kwargs: Parsed to metric function calls.
        Returns:
            dict: The evaluated metrics.
        """
        results = {}
        success = True

        for func in self.functions:
            try:
                # Evaluate the function
                self.logger.debug(f"Calculating metric: {func.__name__}")
                result = func(*args, **kwargs)

                # Make sure the keys are unique
                if any([k in results for k in result]):
                    raise RuntimeError(f"Duplicate key in evaluation of {func.__name__}")

                # Update the output dict
                results.update(result)

            except Exception as err:
                self.logger.warning(f"Exception while evaluating metric {func.__name__}: {err!r}")
                success = False

        return results, success
