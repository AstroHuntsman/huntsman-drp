from lsst.daf.butler import DatasetType, DatasetRef, FileDataset, CollectionType

from huntsman.drp.utils.fits import read_fits_header, parse_fits_header


def get_dataId_from_header(filename, required_keys):
    """ Attempt to get the dataId from its FITS header.
    NOTE: This is a temporary solution for ingesting master calibs.
    Args:
        filename (str): The filename.
        required_keys (iterable of str): The keys to extract.
    Returns:
        dict: The dataId.
    """
    parsed_header = parse_fits_header(read_fits_header(filename))
    return {k: parsed_header[k] for k in required_keys}


def makeFileDataset(datasetType, dataId, filename):
    """ Make a new FileDataset.
    Args:
        datasetType (lsst.daf.butler.DatasetType): The DatasetType object.
        dataId (dict): The dataId.
        filename (str): The filename.
    Returns:
        lsst.daf.butler.FileDataset: The FileDataset object.
    """
    datasetRef = DatasetRef(datasetType, dataId)
    return FileDataset(path=filename, refs=datasetRef)


def ingest_files(butler, datasetType, datasets, collection, transfer="copy"):
    """ Ingest datasets into a Gen3 butler repository collection.
    Args:
        datasetType (lsst.daf.butler.DatasetType): The refcat datasetType.
        datasets (list of lsst.daf.butler.FileDataset): The refcat datasets.
        collection (str): The collection to ingest into.
        transfer (str): The transfer mode. Default: "copy".
    """
    # Register collection and datasetType
    butler.registry.registerCollection(collection, type=CollectionType.RUN)
    butler.registry.registerDatasetType(datasetType)
    # Ingest
    butler.ingest(*datasets, transfer=transfer, run=collection)


def make_calib_dataset_type(datasetTypeName, universe):
    """ Make a DatasetType corresponding to the calib type.
    NOTE: This is a temporary solution for ingesting master calibs.
    Args:
        datasetTypeName (str): The name of the datasetType.
        universe (lsst.daf.butler.DimensionUniverse): The dimension universe.
    Returns:
        lsst.daf.butler.DatasetType: The DatasetType object.
        list of str: The dimension names.
    """
    dimensions = ["instrument", "detector"]

    if datasetTypeName == "flat":
        dimensions.append("physical_filter")

    datasetType = DatasetType(datasetTypeName, dimensions=dimensions, universe=universe,
                              storageClass="ExposureF", isCalibration=True)

    return datasetType, dimensions


def ingest_calibs(butler, datasetTypeName, filenames, collection, **kwargs):
    """ Ingest master calibs into a Butler collection.
    Args:
        butler (lsst.daf.butler.Butler): The butler object.
        filenames (list of str): The files to ingest.
        collection (str): The collection to ingest into.
        **kwargs: Parsed to ingest_files.
    """
    datasetType, dimension_names = make_calib_dataset_type(datasetTypeName,
                                                           universe=butler.registry.dimensions)
    datasets = []
    for filename in filenames:
        dataId = get_dataId_from_header(filename, required_keys=dimension_names)
        datasets.append(makeFileDataset(datasetType, dataId=dataId, filename=filename))

    ingest_files(butler, datasetType, datasets, collection, **kwargs)
