"""
Script to read the data from a vignetting sequence, identify vignetted frames, and produce
a pickled object that can be used to predict vignetted alt/az coordinates.
"""
import argparse
from huntsman.drp.vignetting import VignettingAnalyser


if __name__ == "__main__":

    va = VignettingAnalyser(date="12-10-2020")
    va.create_hull(plot_filename="vignetting_plot.py")
