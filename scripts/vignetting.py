"""
Script to read the data from a vignetting sequence, identify vignetted frames, and produce
a pickled object that can be used to predict vignetted alt/az coordinates.
"""
import argparse
from huntsman.drp.vignetting import VignettingAnalyser

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help='The date of the vignetting sequence observations.')
    parser.add_argument('--hull_filename', default=None, help='Filename of the hull object.')
    parser.add_argument('--plot_filename', default="vignetting_plot.png",
                        help='Filename of the summary plot.')

    args = parser.parse_args()

    va = VignettingAnalyser(date=args.date)
    va.create_hull(filename=args.hull_filename, plot_filename=args.plot_filename)
