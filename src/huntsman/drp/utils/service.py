from huntsman.drp.core import get_config, get_logger
from huntsman.drp.utils import load_module


def create_service(service_class_name, config=None, logger=None, **kwargs):
    """ Create a service instance using the config file to set kwargs.
    Args:
        service_class_name (str): The full Python class name of the service.
        config (dict, optinal): The config. If None (default), will use default config.
        logger (logger, optional): The logger. If None (default), will use default logger.
        **kwargs: Parsed to the service initialiser. Will override config file.
    Returns:
        object: The service instance.
    """
    if config is None:
        config = get_config()
    if logger is None:
        logger = get_logger()

    ServiceClass = load_module(service_class_name)

    # Get config using short service name
    service_name = service_class_name.split(".")[-1]
    service_kwargs = config.get(service_name, {})

    # Update kwargs to allow config overrides
    service_kwargs.update(kwargs)

    logger.info(f"Creating {service_class_name} instance with kwargs: {service_kwargs}")

    # Create the service, parsing config items as kwargs
    return ServiceClass(config=config, logger=logger, **service_kwargs)
