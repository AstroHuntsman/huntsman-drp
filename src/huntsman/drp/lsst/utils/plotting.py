import lsst.afw.display as afwDisplay


def get_display(frame=1, backend="firefly", **kwargs):
    """ Thin wrapper around afwDisplay.Display.
    Args:
        frame (int, optional): The frame identifier. Default: 1.
        backend (str, optional): The display backend. Default: 'firefly'.
        **kwargs: Parsed to afwDisplay.Display.
    Returns:
        afwDisplay.Display: The display object.
    """
    return afwDisplay.Display(frame=frame, backend=backend, **kwargs)


def display_calexp(butler, dataId, collection="calexp", plot_sources=True, marker_size=10,
                   psf_color="orange", photom_color="red", **kwargs):
    """ Display a calexp and sources used for PSF measurement and calibration.
    Args:

    Returns:

    """
    # Get the display
    display = get_display(**kwargs)

    # Display the image
    calexp = butler.get("calexp", dataId=dataId)
    display.mtv(calexp)
    display.scale("asinh", "zscale")

    if plot_sources:
        src = butler.get("src", dataId=dataId).asAstropy().to_pandas()
        xx = src["base_SdssCentroid_x"].values
        yy = src["base_SdssCentroid_y"].values

        # Identify sources used for PSF measurement
        cond_psf = src["calib_psf_used"].values
        with display.Buffering():
            for x, y in zip(xx[cond_psf], yy[cond_psf]):
                display.dot('x', x, y, size=marker_size, ctype=psf_color)

        # Identify sources used for photometric calibration
        cond_photom = src["calib_photometry_used"].values
        with display.Buffering():
            for x, y in zip(xx[cond_photom], yy[cond_photom]):
                display.dot('+', x, y, size=marker_size, ctype=photom_color)

    return display
