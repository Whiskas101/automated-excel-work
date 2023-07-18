import pandas as pd
import numpy as np
import os
import logging


class excelFile:
    #custom excel file class, created for very specific tasks, hence not generalised

    def __init__(self, name, debug=False) -> None:
        self.name = name
        #loading a given excel file
        
        self.dataframe = pd.read_excel(self.name, index_col=None)

        

        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.CRITICAL)

        
        logging.debug(f"Finished loading : '{self.name}'")
        

    
    
    def writeToNewFile(self, name):
        EmptyDataFrame = pd.DataFrame()

        EmptyDataFrame.to_excel(name)



