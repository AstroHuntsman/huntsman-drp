import os

from lsst.ip.isr.defects import Defects

from huntsman.drp.core import get_config


def get_calib_filename(document, config=None, directory=None):
    """ Return the archived calib filename for a calib dataId.
    Args:
        document (abc.Mapping): The mapping with necessary metadata to construct the filename.
        directory (str, optional): The root directory of the calib. If None, use archive dir from
            config.
        config (dict, optional): The config. If None (default), will get default config.
    Returns:
        str: The archived calib filename.
    """
    # Import here to avoid ImportError with screener
    from huntsman.drp.lsst.utils.butler import get_filename_template

    if directory is None:
        if not config:
            config = get_config()
        directory = config["directories"]["archive"]

    # Load the filename template and get the filename
    key = f"calibrations.{document['datasetType']}"
    filename = get_filename_template(key) % document

    return os.path.join(directory, filename)


def make_defects_from_dark(butler, dataId, hot_pixel_threshold):
    """
    """
    # Load the dark image and its mask
    dark = butler.get("dark", dataId=dataId)
    dark_image = dark.getImage().getArray()

    # Load the bad pixel mask already present from the mask
    mask = dark.getMask().clone()
    mask_arr = mask.getArray()

    # Calculate the hot pixel detection threshold in ADU
    satlevel = 2 ** dark.getMetadata()["BITDEPTH"] - 1
    threshold_adu = hot_pixel_threshold * satlevel

    # Identify hot pixels and set the mask bits
    maskBitDet = mask.getPlaneBitMask("BAD")
    mask_arr[dark_image >= threshold_adu] |= maskBitDet

    # Create a LSST defects object
    defects = Defects.fromMask(mask, "BAD")

    # Write the defects object to file inside the butler repo
    dataRef = butler.dataRef("defects", dataId=dataId)
    dataRef.put(defects)
