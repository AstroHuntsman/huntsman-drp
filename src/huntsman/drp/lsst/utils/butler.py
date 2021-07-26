from lsst.daf.butler import DatasetRef, FileDataset, CollectionType

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


def ingest_datasets(butler, datasetType, datasets, collection, transfer="copy"):
    """ Ingest datasets into a Gen3 butler repository collection.
    Args:
        datasetType (lsst.daf.butler.DatasetType): The refcat datasetType.
        datasets (list of lsst.daf.butler.FileDataset): The refcat datasets.
        collection (str): The collection to ingest into.
        transfer (str): The transfer mode. Default: "copy".
    """
    # Register collection
    butler.registry.registerCollection(collection, type=CollectionType.RUN)

    # Ingest datasets
    butler.ingest(*datasets, transfer=transfer, run=collection)


def ingest_calibs(butler, datasetTypeName, filenames, collection, dimension_names, **kwargs):
    """ Ingest master calibs into a Butler collection.
    Args:
        butler (lsst.daf.butler.Butler): The butler object.
        filenames (list of str): The files to ingest.
        collection (str): The collection to ingest into.
        **kwargs: Parsed to ingest_datasets.
    """
    datasetType = butler.registry.getDatasetType(datasetTypeName)

    datasets = []
    for filename in filenames:
        dataId = get_dataId_from_header(filename, required_keys=dimension_names)
        datasets.append(makeFileDataset(datasetType, dataId=dataId, filename=filename))

    ingest_datasets(butler, datasetType, datasets, collection, **kwargs)
