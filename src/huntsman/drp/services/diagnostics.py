import os

import numpy as np
import matplotlib.pyplot as plt

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection

# Metrics to plot histograms per camera
HIST_BY_CAMERA = ("metrics.calexp.psf_fwhm_arcsec",)

# Metrics to plot histograms per camera per filter
HIST_BY_CAMERA_FILTER = ("metrics.calexp.zp_mag",)


class DiagnosticPlotter(HuntsmanBase):
    """
    """
    _hist_by_camera_keys = HIST_BY_CAMERA
    _hist_by_camera_filter_keys = HIST_BY_CAMERA_FILTER

    def __init__(self, find_kwargs=None, **kwargs):

        super().__init__(**kwargs)

        self._image_dir = self.config["directories"]["diagnostics"]
        os.makedirs(self._image_dir, exist_ok=True)

        self._exposure_collection = RawExposureCollection(config=self.config)
        self._calib_collection = MasterCalibCollection(config=self.config)

        find_kwargs = {} if find_kwargs is None else find_kwargs
        self._rawdocs = self._exposure_collection.find(**find_kwargs)
        self._caldocs = self._calib_collection.find(**find_kwargs)

    # Public methods

    def makeplots(self):
        """ Make all plots and write them to the images directory. """

        for key in self._plot_by_camera_keys:
            self.plot_by_camera(key)

        for key in self._plot_by_camera_filter_keys:
            self.plot_by_camera_filter(key)

        for key in self._hist_by_camera_keys:
            self.plot_hist_by_camera(key)

        for key in self._hist_by_camera_filter_keys:
            self.plot_hist_by_camera_filter(key)

    def plot_by_camera(self, x_key, y_key, basename=None, docs=None, linestyle=None,
                       marker="o", markersize=1, **kwargs):
        """
        """
        basename = basename if basename is not None else f"{x_key}_{y_key}"

        if docs is None:
            docs = self._rawdocs

        # Filter documents that have both data for x key and y key
        docs = [d for d in docs if (x_key in d) and (y_key in d)]

        docs_by_camera = self._get_docs_by_camera(docs)

        # Get dict of values organised by camera name
        x_values_by_camera, xmin, xmax = self._get_values_by_camera(x_key, docs_by_camera)
        y_values_by_camera, ymin, ymax = self._get_values_by_camera(y_key, docs_by_camera)

        if not any([_ for _ in x_values_by_camera.values()]):
            self.logger.warning(f"No {x_key} data to make plot for {basename}.")
            return
        if not any([_ for _ in y_values_by_camera.values()]):
            self.logger.warning(f"No {y_key} data to make plot for {basename}.")
            return

        # Make the plot
        fig, axes = self._make_fig_by_camera(n_cameras=len(x_values_by_camera))

        for (ax, cam_name) in zip(axes, x_values_by_camera.keys()):

            x_values = x_values_by_camera[cam_name]
            y_values = y_values_by_camera[cam_name]

            ax.plot(x_values, y_values, linestyle=linestyle, marker=marker, markersize=markersize,
                    **kwargs)

            ax.set_xlim(xmin, xmax)
            ax.set_ylim(ymin, ymax)
            ax.set_title(f"{cam_name}")

        fig.suptitle(basename)

    def plot_hist_by_camera(self, key, basename=None, docs=None):
        """ Plot histograms of quantities by camera.
        Args:
            key (str): Flattened name of the document field to plot.
            basename (str, optional): The file basename. If not provided, key is used.
            docs (list of Document, optional): A list of documents to plot. If None, will use
                self._rawdocs.
        """
        basename = basename if basename is not None else key

        if docs is None:
            docs = self._rawdocs
        docs_by_camera = self._get_docs_by_camera(docs)

        # Get dict of values organised by camera name
        values_by_camera, vmin, vmax = self._get_values_by_camera(key, docs_by_camera)

        if not any([_ for _ in values_by_camera.values()]):
            self.logger.warning(f"No {key} data to make hist for {basename}.")
            return

        # Make the plot
        fig, axes = self._make_fig_by_camera(n_cameras=len(values_by_camera))

        for (ax, (cam_name, values)) in zip(axes, values_by_camera.items()):
            ax.hist(values, range=(vmin, vmax))
            ax.set_title(f"{cam_name}")
        fig.suptitle(basename)

        self._savefig(fig, basename=basename)

    def plot_hist_by_camera_filter(self, key):
        """ Plot histograms of quantities by camera separately for each filter.
        Args:
            key (str): The flattened key to plot.
        """
        filter_names = set([d["filter"] for d in self._rawdocs])

        for filter_name in filter_names:
            docs = [d for d in self._rawdocs if d["filter"] == filter_name]
            basename = f"{key}-{filter_name}"
            self.plot_hist_by_camera(key, basename=basename, docs=docs)

    def plot_by_camera_filter(self, x_key, y_key):
        """ Plot histograms of quantities by camera separately for each filter.
        Args:
            key (str): The flattened key to plot.
        """
        filter_names = set([d["filter"] for d in self._rawdocs])

        for filter_name in filter_names:
            docs = [d for d in self._rawdocs if d["filter"] == filter_name]
            basename = f"{x_key}_{x_key}-{filter_name}"
            self.plot__by_camera(x_key, y_key, basename=basename, docs=docs)

    def _get_docs_by_camera(self, docs):
        """ Return dict of documents with keys of camera name.
        Args:
            docs (list): The list of docs.
        Returns:
            dict: Dict of camera_name: list of docs.
        """
        # Get camera names corresponding to CCD numbers
        cam_configs = self.config["cameras"]["devices"]
        # +1 because ccd numbering starts at 1
        camdict = {i + 1: cam_configs[i]["camera_name"] for i in range(len(cam_configs))}

        docs_by_camera = {}
        for ccd, cam_name in camdict.items():

            camera_docs = [d for d in docs if d["ccd"] == ccd]
            # Drop any cameras with no documents (e.g. testing cameras)
            if not camera_docs:
                self.logger.debug(f"No matching documents for camera {cam_name}.")
                continue

            docs_by_camera[cam_name] = camera_docs

        return docs_by_camera

    def _get_values_by_camera(self, key, docs_by_camera):
        """
        """
        # Get dict of values organised by camera name
        values_by_camera = {}
        vmax = -np.inf
        vmin = np.inf
        for cam_name, docs in docs_by_camera.items():

            # Some measurements may be missing and get will return None
            values = [v for v in [d.get(key) for d in docs] if v is not None]
            values_by_camera[cam_name] = values

            # Update min / max for common range
            if values:
                vmin = min(np.nanmin(values), vmin)
                vmax = max(np.nanmax(values), vmax)

        return values_by_camera, vmin, vmax

    def _make_fig_by_camera(self, n_cameras, n_col=5, figsize=3):
        """ Make a figure with subplots for each camera.
        Args:
            n_cameras (int): The number of cameras.
            n_col (int, optional): The number of columns in the figure.
            figsize (int, optional): The size of each panel. Default: 3.
        Returns:
            matplotlib.pyplot.Figure: The figure object.
            list of matplotlib.pyplot.Axes: The axes for each subplot.
        """
        n_row = int((n_cameras - 1) / n_col) + 1
        fig = plt.figure(figsize=(n_col * figsize, n_row * figsize))

        axes = []
        for i in range(n_row):
            for j in range(n_col):
                axes.append(fig.add_subplot(n_row, n_col, i * n_col + j + 1))

        return fig, axes

    def _savefig(self, fig, basename, dpi=150, tight_layout=True):
        """ Save figure to images directory.
        Args:
            fig (matplotlib.pyplot.Figure): The figure to save.
            basename (str): The basename of the file to save the image to.
            tight_layout (bool, optional): If True (default), use tight layout for figure.
            **kwargs: Parsed to fig.savefig.
        """
        basename = basename.replace(".", "-") + ".png"  # Remove nested dict dot notation
        filename = os.path.join(self._image_dir, basename)
        self.logger.debug(f"Writing image: {filename}")

        if tight_layout:
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])

        fig.savefig(filename, dpi=dpi, bbox_inches="tight")
