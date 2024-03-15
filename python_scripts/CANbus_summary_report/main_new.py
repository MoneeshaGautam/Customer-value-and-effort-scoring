# Import the libraries

import pandas as pd
import warnings
from utilities import *
#from manipulator import manipulation
from manipulation import manipulation
from extractor import extract_data
#%run ./credentionals.ipynb import *
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)


manipulation_output=manipulation()
#data_analysis = pd.read_csv(manipulation_output )
#manipulation_output.to_csv('data_analysis.csv')
#one_day_data=extract_data()
#one_day_data.to_csv('one_day_data.csv')
