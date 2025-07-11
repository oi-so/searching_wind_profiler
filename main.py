from glob import glob
import os
from pybufrkit.decoder import Decoder
from pybufrkit.renderer import FlatJsonRenderer
from bufrConverter import BufrConverter
from pprint import pprint



bu = BufrConverter()
bu.convert()
bu.split_data()
bu.get_date()