""" Base class for all huntsman-drp classes which provides the default logger and config."""

from huntsman.drp.core import get_config, get_logger


class HuntsmanBase():

    _date_key = "date"
    _config_name = None

    def __init__(self, config=None, logger=None):
        self.logger = get_logger() if logger is None else logger
        self.config = get_config() if config is None else config

    @classmethod
    def from_config(cls, config=None, logger=None, **kwargs):
        """ Create a class instance from config.
        Args:
            config (dict, optional): The config. If None, use default config.
            logger (logger, optional): The logger. If None, use default logger.
            **kwargs: Parsed to initialiser, overriding config.
        Returns:
            object: The configured class instance.
        """
        config_name = cls.__name__ if cls._config_name is None else cls._config_name

        config = get_config() if config is None else config
        logger = get_logger() if logger is None else logger

        instance_kwargs = config.get(config_name, {})
        instance_kwargs.update(**kwargs)

        logger.debug(f"Creating {cls.__name__} instance with kwargs: {instance_kwargs}")

        return cls(config=config, logger=logger, **instance_kwargs)
