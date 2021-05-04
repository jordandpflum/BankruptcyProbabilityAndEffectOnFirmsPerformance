"""
Wrapper that creates the Y-Dataframe from raw data
02-02-2021
"""

import pandas as pd
import numpy as np

def bankruptcyWithinNMonths(row, N, filler):
  """
  Check if a Bankruptcy has occured within N months of a given date (month)
  """
  dldte_month = row['dldte_month']
  date_month = row['date_month']
  check_date = date_month+N
  
  bankruptcyOccurs=0
  if check_date>= dldte_month:
    bankruptcyOccurs=1

  return bankruptcyOccurs
  
def createYDataFrame(xDataFrame, 
                     monthsWithinBankruptcy = [3, 6, 12, 24, 60],
                     dropNA=True,
                     featuresToKeep =['PERMNO', 'GVKEY', 'conm', 'date_month']
                     ):
  """
  Create Y DataFrame
  """
  # Create Y-Dataframe
  yDataFrame = xDataFrame.copy()

  if dropNA:
    # Drop NAs
    yDataFrame = yDataFrame.dropna()

  # Format Deletion Date
  yDataFrame['dldte'] = pd.to_datetime(yDataFrame['dldte'])

  # Convert Date to Month Period
  yDataFrame['dldte_month'] = yDataFrame['dldte'].dt.to_period('m')


  monthsWithinBankruptcy = monthsWithinBankruptcy
  bankruptcyIndicators = []

  for monthLag in monthsWithinBankruptcy:
    # Determine new column name
    colName = f'bankruptcyWithin{monthLag}Months'

    # Append Bankruptcy Indicator to list
    bankruptcyIndicators.append(colName)

    # Initially Fill Columns to 0
    yDataFrame[colName] = 0

    # Update indicator
    #yDataFrame.update(yDataFrame[yDataFrame['dlrsn']==2].apply(bankruptcyWithinNMonths, axis=1, args=(monthLag, 0)))
    yDataFrame.loc[yDataFrame['dlrsn']==2, colName] = yDataFrame[yDataFrame['dlrsn']==2].apply(bankruptcyWithinNMonths, axis=1, args=(monthLag, 0))

  # Keep only desired features
  featuresToKeep = featuresToKeep
  featuresToKeep.extend(bankruptcyIndicators)
  yDataFrame = yDataFrame[featuresToKeep]
  
  return yDataFrame