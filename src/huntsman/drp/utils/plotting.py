import matplotlib.pyplot as plt


def plot_wcs_box(document, ax, **kwargs):
    """ Plot the boundaries of the image in WCS coordinates.
    Args:
        documents (RawExposureDocument: The document to plot.
        ax (matplotlib.Axes): The axes instance.
        **kwargs: Parsed to matplotlib.pyplot.plot.
    """
    # Get the WCS
    wcs = document.get_wcs()

    # Get boundaries
    bl = wcs.pixel_to_world(0, 0)
    br = wcs.pixel_to_world(document["NAXIS1"], 0)
    tr = wcs.pixel_to_world(document["NAXIS1"], document["NAXIS2"])
    tl = wcs.pixel_to_world(0, document["NAXIS2"])

    # Plot box
    ax.plot(*[[bl[i], tl[i]] for i in range(2)])
    ax.plot(*[[tl[i], tr[i]] for i in range(2)])
    ax.plot(*[[tr[i], br[i]] for i in range(2)])
    ax.plot(*[[br[i], bl[i]] for i in range(2)])


def plot_wcs_boxes(documents, color="k", linewidth=1, **kwargs):
    """ Plot the boundaries of the images in WCS coordinates.
    Args:
        documents (list of RawExposureDocument): The documents to plot.
        **kwargs: Parsed to matplotlib.pyplot.plot.
    Returns:
        matplotlib.Figure, matplotlib.Axes: The figure and axes.
    """
    fig, ax = plt.subplots()

    for document in documents:
        plot_wcs_box(document, ax, color=color, linewidth=linewidth, **kwargs)

    ax.set_xlabel("RA [deg]")
    ax.set_ylabel("Dec [deg]")

    return fig, ax
