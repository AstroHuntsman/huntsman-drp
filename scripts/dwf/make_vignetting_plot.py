"""
Make vignetting test plot for flat fields.
"""
from multiprocessing import Pool
import matplotlib.pyplot as plt
from astropy.io import fits

from huntsman.drp.datatable import RawDataTable
from huntsman.drp.quality.vignetting import calculate_asymmetry_statistics

INTERVAL = 3
OUTPUT_FILENAME = "vignetting_flats.png"
CCD = 2
FILTER_NAME = "g_band"
NPROC = 8


def calculate_asymmetry(filename):
    """Load data and calculate statistics."""
    data = fits.getdata(filename)
    return calculate_asymmetry_statistics(data)


if __name__ == "__main__":

    # Move these to script args
    interval_days = INTERVAL
    output_filename = OUTPUT_FILENAME
    ccd = CCD
    filter_name = FILTER_NAME

    # Get recent flat field images
    datatable = RawDataTable()
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query_latest(days=interval_days, dataType="science", ccd=ccd,
                                      FILTER=filter_name)
    filenames = []
    for m in metalist:
        if m["FIELD"].startswith("Flat"):
            filenames.append(m["filename"])

    # Calculate asymmetry statistics
    print(f"Processing {len(filenames)} flat fields.")
    with Pool(NPROC) as pool:
        results = pool.map(calculate_asymmetry, filenames)

    plt.figure()
    x = [r[0] for r in results]
    y = [r[1] for r in results]
    plt.plot(x, y, 'k+')
    plt.savefig(OUTPUT_FILENAME, bbox_inches="tight", dpi=150)
