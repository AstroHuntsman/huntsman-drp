from astropy import stats
import astropy.io.fits as fits


def get_metadata(data):
    """
    Return a dictionary of simple image stats for the file.
    Args:
        filename (str): Filename of FITS image.
    Returns:
        dict: A dictionary of metadata key: value pairs.
    """
    result = dict()

    mean, median, stdev = stats.sigma_clipped_stats(data)
    result["mean_counts"] = mean
    result["median_counts"] = median
    result["std_counts"] = stdev

    return result


def calculate_asymmetry_statistics(data):
    """
    Calculate the asymmetry statistics by flipping data in x and y directions.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data-data_flip).std()

    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data-data_flip).std()

    return std_horizontal, std_vertical


def get_quality_metadata(filename_list):
    """
    Return a dictionary of simple stats for all fits filenames in the input list.
    Args:
        filename_list (list): List of fits filenames.
    Returns:
        list of dict: Data quality metadata for each file.
    """
    result = []
    for filename in filename_list:
        data = fits.getdata(filename)
        md = get_quality_metadata(data)
        result.append(md)
    return result
