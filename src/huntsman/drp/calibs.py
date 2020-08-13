
from astropy import stats
import astropy.io.fits as fits


def get_calib_data_qualities(filename_list):
    output_data_quality_dict = {}
    for filename in filename_list:
        mean, median, stdev = stats.sigma_clipped_stats(fits.getdata(filename))
        output_data_quality_dict[filename] = (mean, median, stdev)
    return(output_data_quality_dict)
