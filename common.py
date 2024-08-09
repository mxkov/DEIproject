import io
import pandas as pd
from math import floor


DATE_START = "1980-01-01"
DATE_END   = "2024-05-31"


def read_datafile(filename, zip_file,
                  remove_missing=True, remove_suspect=False):
	"""Extract a dataframe from one datafile from a ZIP archive"""
	f = io.TextIOWrapper(zip_file.open(filename))
	lines = f.read().split("\n")[20:]
	assert lines[0].startswith("STAID, SOUID,    DATE,")

	df = pd.read_csv(io.StringIO("\n".join(lines)))
	df.columns = [x.strip() for x in df.columns]
	df.DATE = pd.to_datetime(df.DATE, format="%Y%m%d")

	if remove_missing:
		df = df[df.iloc[:,-1] != 9]
	if remove_suspect:
		df = df[df.iloc[:,-1] != 1]

	return df


def convert_coord(x):
	"""Convert latitude or longitude from degrees:minutes:seconds to degrees with decimals"""
	x1 = x[1:].split(":")
	x1 = [int(val) for val in x1]
	res = x1[0] + x1[1]/60 + x1[2]/3600
	assert floor(res)==x1[0]
	return res
