from excelFile import excelFile
import pandas as pd
import numpy as np
import logging
from time import time
from time import perf_counter


logging.basicConfig(level=logging.DEBUG)

#separating functions into this file to make main file cleaner

def updateGRN():

    logging.debug("Loading main file...")
    mainFile = excelFile("Main File\ORDER VS GRN OBD AND BILLING DATA.xlsx", debug=True)
    
    mainDataFrame = mainFile.dataframe
    mainDataFrame = mainDataFrame.loc[: ,"SEASON":"BAL VALUE"]
    mainDataFrame = cleanData(mainDataFrame, 'STYLE+COL')

    codeMap1 = mainDataFrame[mainDataFrame.columns.intersection(['STYLE+COL', 'NET ORDER QTY'])]
    codeMap1 = codeMap1.groupby(['STYLE+COL'])
    codeMap1 = codeMap1.sum()

    #codeMap1.to_csv("temp1.csv")

    logging.debug("Loaded main file.")

    columnToSum = 'ORDER QTY'
    totalQuantity = mainDataFrame[columnToSum].sum()
    logging.debug(f"total quantity = {totalQuantity}")
    

    logging.debug("Loading GRN DATA file...")

    grnFile = excelFile("GRN Data\GRN DATA.xlsb", debug=True)
    grnDataFrame = grnFile.dataframe

    logging.debug("Loaded GRN DATA file.")


    codeMap2 = grnDataFrame[grnDataFrame.columns.intersection(['STYLE+COL',"QTY"])] 

    print(codeMap2.head())
    
    
    logging.debug("Grouping")
    codeMap2 = codeMap2.groupby(['STYLE+COL'])
    codeMap2 = codeMap2.sum()

    print(codeMap1.head())
    print(codeMap2.head())
    
    final_map = pd.merge(codeMap1, codeMap2, how="inner", on="STYLE+COL")
    final_map = cleanData(final_map, "QTY")
    
    final_map["GRN percent"] = (final_map["QTY"]/final_map["NET ORDER QTY"])
    

    final_map = final_map.drop(["NET ORDER QTY"], axis=1)
    print(final_map.head())
    
    #final_map.to_csv("final_map.csv")
    
    newDataFrame = final_map[final_map.columns.intersection(["STYLE+COL", "GRN percent"])]

    result = mainDataFrame.merge(newDataFrame, how="left", on="STYLE+COL")
    result.to_csv("GRN DATA MERGED.csv")

    logging.debug("Finished with GRN update")

    

    
    

def cleanData(DataFrame : pd.DataFrame, colName : str):
    """
        Cleans data by looking through a specific column and removes a row, if the given cell in the column
        is empty.
    """
    DataFrame = DataFrame.loc[DataFrame[colName].notna()]
    return DataFrame




def OBD():

    logging.debug("Loading OBD File...")

    obdFile = excelFile("OBD DATA\OBD DATA.xlsx") 
    obdDataFrame = obdFile.dataframe
    obdDataFrame = obdDataFrame[obdDataFrame.columns.intersection(["CHANNEL","PARTNER","STYLE+COL", "OBD_QTY"])]

    obdDataFrame = obdDataFrame.groupby(["CHANNEL","PARTNER","STYLE+COL"]).sum()

    mainDataFrame = pd.read_csv("GRN DATA MERGED.csv")
    
    mainDataFrame = mainDataFrame[mainDataFrame.columns.intersection(["CHANNEL","PARTNER","STYLE+COL"])]
   

    segregated_data = obdDataFrame.merge(mainDataFrame, on=["CHANNEL","PARTNER","STYLE+COL"], how='left').dropna()

    #removing those rows whose style+col codes do not exist in the main file
    test= pd.DataFrame(mainDataFrame["STYLE+COL"])

    #test = stylecol.merge(segregated_data, on=["STYLE+COL"], how="inner")
    test = test[test.columns.intersection(["STYLE+COL"])]
    test = test.drop_duplicates()
    
    segregated_data = segregated_data.merge(test, how="inner" ,on="STYLE+COL")
    #segregated_data.to_csv("Result-after.csv")

    
    orgFileDataFrame = pd.read_csv("GRN DATA MERGED.csv")
    orgFileDataFrame = orgFileDataFrame.merge(segregated_data, on=["STYLE+COL" ,"CHANNEL", "PARTNER"], how="outer")
   
    orgFileDataFrame.to_csv("UPDATED OBD AND GRN.csv")   

    logging.debug("Finished OBD Processing")
    #resultDataFrame.to_csv('Result.csv')





    
def BillingData():
    
    BillingFile = excelFile("BillingData\BILLING DATA.xlsx", debug=True)
    BillingDataFrame = BillingFile.dataframe

    mainDataFrame = pd.read_csv("UPDATED OBD AND GRN.csv")
    mainDataFrame = mainDataFrame[mainDataFrame.columns.intersection(["CHANNEL","PARTNER","STYLE+COL"])]

    #Grouping data according to Channel, partner and Style code
    BillingDataFrame = BillingDataFrame[BillingDataFrame.columns.intersection(["QTY","CHANNEL","PARTNER","STYLE+COL"])]
    groupedBillingData = BillingDataFrame.groupby(["CHANNEL","PARTNER","STYLE+COL"]).sum()
    #groupedBillingData.to_csv("Billing data grouped.csv")


    
    segregated_data = groupedBillingData.merge(mainDataFrame,on=["CHANNEL","PARTNER","STYLE+COL"], how="left", indicator=True)
    
    segregated_data.to_csv("GroupedDataUnedited.csv")
    test= pd.DataFrame(mainDataFrame["STYLE+COL"])

    
    test = test[test.columns.intersection(["STYLE+COL"])]
    test = test.drop_duplicates()
    
    segregated_data = segregated_data.merge(test, how="inner", on="STYLE+COL")
    #segregated_data.to_csv("grouped-data.csv")

    finalFile = pd.read_csv("UPDATED OBD AND GRN.csv")
    finalFile = finalFile.merge(segregated_data, on=["STYLE+COL" ,"CHANNEL", "PARTNER"], how="outer")
    
    finalFile.to_csv("Updated OBD GRN & BILLING DATA.csv")



   









start = perf_counter()
updateGRN()
OBD()
BillingData()
end = perf_counter()

print(f"Took {end - start}s ")


