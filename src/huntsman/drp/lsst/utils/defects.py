from scipy.ndimage.morphology import binary_dilation

from lsst.ip.isr.defects import Defects


def make_defects_from_dark(butler, dataId, hot_pixel_threshold):
    """ Make a defects file from a master dark and put it into the butler repository.
    Args:
        butler (lsst.daf.persistence.butler.Butler): The butler object.
        dataId (dict): The dataId of the master dark.
        hot_pixel_threshold (float): The fraction of the saturation level required for a pixel
            to be regarded as hot.
    """
    # Load the dark image and its mask
    dark = butler.get("dark", dataId=dataId)
    dark_image = dark.getImage().getArray()

    # Calculate the hot pixel detection threshold in ADU
    satlevel = 2 ** dark.getMetadata()["BITDEPTH"] - 1
    threshold_adu = hot_pixel_threshold * satlevel

    # Identify hot pixels and set the mask bits
    mask = dark.getMask().clone()
    mask_arr = mask.getArray()
    maskBitDet = mask.getPlaneBitMask("BAD")
    mask_arr[dark_image >= threshold_adu] |= maskBitDet

    # Dilate the bad pixel mask using default connectivity 1 kernel
    mask_arr = binary_dilation(mask_arr)

    # Create a LSST defects object
    defects = Defects.fromMask(dark.getMaskedImage(), "BAD")

    # Write the defects object to file inside the butler repo
    dataRef = butler.dataRef("defects", dataId=dataId)
    dataRef.put(defects)
