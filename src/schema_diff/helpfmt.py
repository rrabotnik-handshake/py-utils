import argparse
class ColorDefaultsFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter):
    def __init__(self, *a, **k):
        k.setdefault("max_help_position", 40)
        k.setdefault("width", 100)
        super().__init__(*a, **k)
