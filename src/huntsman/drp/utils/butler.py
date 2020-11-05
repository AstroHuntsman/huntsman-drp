import os
from lsst.daf.persistence import FsScanner


def get_filename_template(datasetType, policy):
    """ Get the filename template for a specific datatype.
    Args:
        datasetType (str): The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
        policy (`lsst.daf.persistence.Policy`): The Policy object.
    Returns:
        str: The filename template.
    """
    policy_key = datasetType + ".template"
    template = policy[policy_key]
    if template is None:
        raise KeyError(f"Template not found for {datasetType}.")
    return template


def get_files_of_type(datasetType, directory, policy):
    """
    Get the filenames of a specific dataset type under a particular directory that match
    the appropriate filename template.
    Args:
        datasetType (str): The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
        directory (str): The directory to search under (e.g. a butler directory).
        policy (`lsst.daf.persistence.Policy`): The Policy object.
    Returns:
        list of dict: The matching data IDs.
        list of str: The matching filenames.
    """
    # Get the filename tempalte for this file type
    template = get_filename_template(datasetType, policy)

    # Find filenames that match the template in the directory
    scanner = FsScanner(template)
    matches = scanner.processPath(directory)
    data_ids = list(matches.values())
    filenames = [os.path.join(directory, f) for f in matches.keys()]

    return data_ids, filenames
