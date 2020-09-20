"""
Make focus position plot for recent science images.
"""
import matplotlib.pyplot as plt
import numpy as np

from huntsman.drp.datatable import RawDataTable

INTERVAL = 7
OUTPUT_FILENAME = "focus_positions.png"
BINWIDTH = 10

if __name__ == "__main__":

    # Move these to script args
    interval_days = INTERVAL
    output_filename = OUTPUT_FILENAME

    # Get recent science images
    datatable = RawDataTable()
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query_latest(days=interval_days, dataType="science")
    focus_positions = []
    ccd_names = []
    for m in metalist:
        if not m["FIELD"].startswith("Flat"):
            focus_positions.append(m["FOC-POS"])
            ccd_names.append(m["INSTRUME"])
    unique_ccd_names = np.unique(ccd_names)

    plt.figure(figsize=(4, 4*len(unique_ccd_names)))
    rng = min(focus_positions), max(focus_positions)
    nbins = (rng[1]-rng[0])/BINWIDTH
    for i, ccd_name in enumerate(unique_ccd_names):
        ax = plt.subplot(len(unique_ccd_names), 1, i+1)
        x = [f for c, f in zip(ccd_names, focus_positions) if c == ccd_name]
        ax.hist(x, range=rng, density=False, bins=nbins)
        ax.set_title(ccd_name)
    ax.set_xlabel("Focus Position")
    plt.savefig(OUTPUT_FILENAME, bbox_inches="tight", dpi=150)
