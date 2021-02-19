"""
Wrapper that creates the X-Dataframe from raw data
02-01-2021
"""

import pandas as pd
import numpy as np


def prepareCrspCompustatMergedData(CRSP_COMPUSTAT_MERGED, monthsToLagAccountingVariables=2):
  """
  Format and manipulate CRSP/COMPUSTAT Merged Data
  """
  CRSP_COMPUSTAT_MERGED_COPY = CRSP_COMPUSTAT_MERGED.copy()
  # Filter out 6000-6999 Range Companies and make sure traded on American Exchange
  CRSP_COMPUSTAT_MERGED_COPY = CRSP_COMPUSTAT_MERGED_COPY[((CRSP_COMPUSTAT_MERGED_COPY['sic']<6000) |  (CRSP_COMPUSTAT_MERGED_COPY['sic']>=7000)) &
                                                          (CRSP_COMPUSTAT_MERGED_COPY['exchg'].isin([11, 12, 13, 14, 15, 16, 17, 18, 19, 20]))                  
                                                          ]

  # Split Up 'datacqtr' into calendar year and quarter
  CRSP_COMPUSTAT_MERGED_COPY['CalendarYear'] = CRSP_COMPUSTAT_MERGED_COPY['datacqtr'].str.slice(0,4)
  CRSP_COMPUSTAT_MERGED_COPY['Quarter'] = CRSP_COMPUSTAT_MERGED_COPY['datacqtr'].str.slice(4)

  # Convert Quarters to Pandas Datetimes
  CRSP_COMPUSTAT_MERGED_COPY['datacqtr_formatted'] = CRSP_COMPUSTAT_MERGED_COPY["CalendarYear"] + "-" + CRSP_COMPUSTAT_MERGED_COPY["Quarter"]
  CRSP_COMPUSTAT_MERGED_COPY['QuarterStart'] = pd.PeriodIndex(CRSP_COMPUSTAT_MERGED_COPY['datacqtr_formatted'], freq='Q').to_timestamp()

  # Calculate Start of Quarter and End of Quarter Dates
  CRSP_COMPUSTAT_MERGED_COPY['QuarterEnd'] = CRSP_COMPUSTAT_MERGED_COPY['QuarterStart'] + pd.offsets.MonthEnd(3)
  CRSP_COMPUSTAT_MERGED_COPY['QuarterStart_Month'] = CRSP_COMPUSTAT_MERGED_COPY['QuarterStart'].dt.to_period('m')
  CRSP_COMPUSTAT_MERGED_COPY['QuarterEnd_Month'] = CRSP_COMPUSTAT_MERGED_COPY['QuarterEnd'].dt.to_period('m')

  # Calculate Lagged Dates (Year-Month)
  for i in range(3):
    lag = monthsToLagAccountingVariables
    CRSP_COMPUSTAT_MERGED_COPY[f'Date_Lag{i+lag}'] = CRSP_COMPUSTAT_MERGED_COPY['QuarterEnd_Month'] + (lag+i)

  return CRSP_COMPUSTAT_MERGED_COPY

def prepareCrspMonthlyData(CRSP_MONTHLY):
  """
  Format and manipulate CRSP Monthly Data
  """

  # Create Copy of Dataframe
  CRSP_MONTHLY_COPY = CRSP_MONTHLY.copy()

  # Convert to Datetime
  CRSP_MONTHLY_COPY['date'] = pd.to_datetime(CRSP_MONTHLY_COPY['date'])

  # Convert Date to Month Period
  CRSP_MONTHLY_COPY['date_month'] = CRSP_MONTHLY_COPY['date'].dt.to_period('m')

  # Filter Out 6000 Range SIC Companies (Financial and ETFs)
  CRSP_MONTHLY_COPY['SICCD'] = pd.to_numeric(CRSP_MONTHLY_COPY['SICCD'], errors='coerce')
  CRSP_MONTHLY_COPY = CRSP_MONTHLY_COPY[(CRSP_MONTHLY_COPY['SICCD']<6000) |  (CRSP_MONTHLY_COPY['SICCD']>=7000)]

  # Filter Share Code to be 10 or 11
  CRSP_MONTHLY_COPY['SHRCD'] = pd.to_numeric(CRSP_MONTHLY_COPY['SHRCD'], errors='coerce')
  CRSP_MONTHLY_COPY = CRSP_MONTHLY_COPY[(CRSP_MONTHLY_COPY['SHRCD'].isin([10,11]))]

  # Filter Share Class to be 'A' or NaN
  CRSP_MONTHLY_COPY = CRSP_MONTHLY_COPY[((CRSP_MONTHLY_COPY['SHRCLS'].isna()) | (CRSP_MONTHLY_COPY['SHRCLS'] == 'A'))]

  # Filter Out Returns less than -50
  CRSP_MONTHLY_COPY['RET'] = pd.to_numeric(CRSP_MONTHLY_COPY['RET'], errors='coerce')
  CRSP_MONTHLY_COPY = CRSP_MONTHLY_COPY[(CRSP_MONTHLY_COPY['RET']>-50)]

  return CRSP_MONTHLY_COPY

def prepareCrspDailyData(CRSP_DAILY):
  """
  Format and manipulate CRSP (Daily) Data
  """
  # Create Copy of Dataframe
  CRSP_DAILY_COPY = CRSP_DAILY.copy()

  # Convert Date to Month Period
  CRSP_DAILY_COPY['date'] = pd.to_datetime(CRSP_DAILY_COPY['date'])
  CRSP_DAILY_COPY['date_month'] = CRSP_DAILY_COPY['date'].dt.to_period('m')

  return CRSP_DAILY_COPY


def prepareSP500Data(SP500_MONTHLY):
  """
  Format and manipulate SP500 Monthly Data
  """
  # Create Copy of Dataframe
  SP500_MONTHLY_COPY = SP500_MONTHLY.copy()

  # Format caldt as datetime
  SP500_MONTHLY_COPY['caldt'] = pd.to_datetime(SP500_MONTHLY_COPY['caldt'])

  # Convert Date to Month Period
  SP500_MONTHLY_COPY['date_month'] = SP500_MONTHLY_COPY['caldt'].dt.to_period('m')

  # Adjust totval (quoted in $1000s)
  SP500_MONTHLY_COPY['totval'] = SP500_MONTHLY_COPY['totval']*1000

  # Drop caldt
  SP500_MONTHLY_COPY = SP500_MONTHLY_COPY.drop(columns=['caldt'])

  # Rename Columns (to make later merge simpler)
  SP500_MONTHLY_COPY = SP500_MONTHLY_COPY.rename(columns={'vwretd': 'vwretdSP500', 'totval': 'totvalSP500'})

  return SP500_MONTHLY_COPY

def mergeCrspCompustatMergedWithCrspMonthly(CRSP_COMPUSTAT_MERGED, 
                                            CRSP_MONTHLY,
                                            CRSP_COMPUSTAT_Accounting_features = ['atq', 'ceqq', 'cheq', 'ltq', 'niq'],
                                            CRSP_COMPUSTAT_Identifying_features = ['GVKEY', 'conm'],
                                            CRSP_MONTHLY_features = ['PERMNO', 'date_month', 'PRC', 'SHROUT', 'CFACPR', 'RET']
                                            ):
  """
  Merge CRSP/COMPUSTAT Merged Dataframe with CRSP (Monthly)
  """
  # Create Copy of Dataframe
  CRSP_COMPUSTAT_MERGED_COPY = CRSP_COMPUSTAT_MERGED.copy()
  CRSP_MONTHLY_COPY = CRSP_MONTHLY.copy()

  # Select Accounting Features to Merge into CRSP Monthly Dataframe
  CRSP_COMPUSTAT_Accounting_features = CRSP_COMPUSTAT_Accounting_features

  # Select Identifying Information Features to Merge into CRSP Monthly Dataframe
  CRSP_COMPUSTAT_Identifying_features = CRSP_COMPUSTAT_Identifying_features

  # Select CRSP Features to Keep after Merge
  CRSP_MONTHLY_features = CRSP_MONTHLY_features

  # Select Features to Keep after Merge
  featuresToKeep = CRSP_MONTHLY_features.copy()

  # Add Accounting Features to Features to keep after merge
  featuresToKeep.extend(CRSP_COMPUSTAT_Accounting_features.copy())

  # Add Identifying Information Features to Features to keep after merge
  featuresToKeep.extend(CRSP_COMPUSTAT_Identifying_features.copy())

  # Add Lagged Accounting Features
  for lag in range(2,5):
      CRSP_COMPUSTAT_merge_features = ['LPERMNO', f'Date_Lag{lag}']
      CRSP_COMPUSTAT_merge_features.extend(CRSP_COMPUSTAT_Accounting_features.copy())
      CRSP_COMPUSTAT_merge_features.extend(CRSP_COMPUSTAT_Identifying_features.copy())
      if lag==2:
          temp = pd.merge(CRSP_COMPUSTAT_MERGED_COPY[CRSP_COMPUSTAT_merge_features],
                                  CRSP_MONTHLY_COPY[CRSP_MONTHLY_features],
                                  how='right',
                                  left_on=['LPERMNO', f'Date_Lag{lag}'],
                                  right_on=['PERMNO', 'date_month']
                                  )
          # Keep only specified Features
          temp = temp[featuresToKeep]
          
      else:
          temp = pd.merge(CRSP_COMPUSTAT_MERGED_COPY[CRSP_COMPUSTAT_merge_features],
                                  temp[featuresToKeep],
                                  how='right',
                                  left_on=['LPERMNO', f'Date_Lag{lag}'],
                                  right_on=['PERMNO', 'date_month'],
                          suffixes=('', '_y')
                                  )
                        
          # Update Features (Fill NAs with Lagged Variable)
          CRSP_COMPUSTAT_features = CRSP_COMPUSTAT_Accounting_features.copy()
          CRSP_COMPUSTAT_features.extend(CRSP_COMPUSTAT_Identifying_features.copy())

          for feature in CRSP_COMPUSTAT_features:
              temp[feature] = temp[feature].fillna(temp[f'{feature}_y'])
              temp = temp.drop([f'{feature}_y'], 1)
          
          temp = temp[featuresToKeep]

  explanatoryDataFrame = temp.copy()
  return explanatoryDataFrame

def mergeExplanatoryDataframeWithCrspDaily(explanatoryDataFrame, CRSP_DAILY):
  """
  Merge Existing Explanatory Dataframe with CRSP (Daily) Dataframe
  """
  explanatoryDataFrame = pd.merge(explanatoryDataFrame,
                CRSP_DAILY[['PERMNO', 'date_month', 'SIGMA']],
                how='left',
                left_on=['PERMNO', 'date_month'],
                right_on=['PERMNO', 'date_month']
               )
  
  return explanatoryDataFrame

def mergeExplanatoryDataframeWithSP500Monthly(explanatoryDataFrame, SP500_MONTHLY):
  """
  Merge Existing Explanatory Dataframe with SP500 (Monthly) Dataframe
  """
  explanatoryDataFrame = pd.merge(explanatoryDataFrame,
                SP500_MONTHLY,
                how='left',
                left_on=['date_month'],
                right_on=['date_month']
               )
  
  return explanatoryDataFrame

def calculateNITA(PRC, SHROUT, CEQQ, ATQ, NIQ):
  """
  Calculate NITA
  """
  # Precomuputations
  ME = PRC * SHROUT
  BE = CEQQ
  TA = ATQ

  # Total Assets Adjusted Calculation
  totalAssetsAdj = TA + 0.1*(ME - BE)

  # NITA Calculation
  NITA = NIQ / totalAssetsAdj

  return NITA


def calculateNIMTA(PRC, SHROUT, NIQ, LTQ):
  """
  Calculate NIMTA
  """
  # Precomputations
  ME = PRC * SHROUT

  # NIMTA Calculation
  NIMTA = NIQ / (ME + LTQ)

  return NIMTA

def calculateTLTA(PRC, SHROUT, CEQQ, ATQ, LTQ):
  """
  Calculate TLTA
  """
  # Precomuputations
  ME = PRC * SHROUT
  BE = CEQQ
  TA = ATQ

  # Total Assets Adjusted Calculation
  totalAssetsAdj = TA + 0.1*(ME - BE)

  # TLTA Calculation
  TLTA = LTQ / totalAssetsAdj

  return TLTA

def calculateTLMTA(PRC, SHROUT, LTQ):
  """
  Calculate TLMTA
  """
  # Precomuputations
  ME = PRC * SHROUT

  # TLMTA Calculation
  TLMTA = LTQ / (ME + LTQ)

  return TLMTA

def calculateEXRET(PRC, CFACPR, VWRETDSP500):
  """
  Calculate EXRET
  """
  # Precomputations
  ADJPRC = PRC * CFACPR
  RET = ADJPRC.shift(1) / ADJPRC

  # EXRET Calculation
  EXRET = np.log(1+RET) - np.log(1+VWRETDSP500)

  return EXRET

def calculateRSIZE(PRC, SHROUT, TOTVALSP500):
  """
  Calcuate RSIZE
  """
  # Precomuputations
  ME = PRC * SHROUT

  # RSIZE Calculation
  RSIZE = ME / TOTVALSP500

  return RSIZE

def calculateCASHMTA(PRC, SHROUT, CHEQ, LTQ):
  """
  Calculate CASHMTA
  """
  # Precomuputations
  ME = PRC * SHROUT

  # CASHMTA Calculation
  CASHMTA = CHEQ / (ME + LTQ)

  return CASHMTA
  
def createCustomExplanatoryVariables(explanatoryDataFrame, 
                                     explanatoryVariablesToCalculate=['NITA', 
                                                                      'NIMTA',
                                                                      'TLTA',
                                                                      'TLMTA',
                                                                      'EXRET',
                                                                      'RSIZE',
                                                                      'CASHMTA',
                                                                      'SIGMA'
                                                                      ],
                                     identifyingColumns = ['PERMNO',
                                                           'GVKEY',
                                                           'conm'
                                                           ],
                                     keepAllFeatures=False
                                     ):
  """
  Create Custom Explanatory Variables and add them to existing explanatory 
  Dataframe
  """
  if 'NITA' in (explanatoryVariablesToCalculate):
    # Calculate NITA
    explanatoryDataFrame['NITA'] = calculateNITA(PRC=explanatoryDataFrame['PRC'], 
                                                SHROUT=explanatoryDataFrame['SHROUT'], 
                                                CEQQ=explanatoryDataFrame['ceqq'], 
                                                ATQ=explanatoryDataFrame['atq'], 
                                                NIQ=explanatoryDataFrame['niq']
                                                )
  
  if 'NIMTA' in (explanatoryVariablesToCalculate):
    # Calculate NIMTA
    explanatoryDataFrame['NIMTA'] = calculateNIMTA(PRC=explanatoryDataFrame['PRC'], 
                                                SHROUT=explanatoryDataFrame['SHROUT'],
                                                NIQ=explanatoryDataFrame['niq'],
                                                LTQ=explanatoryDataFrame['ltq']
                                                )
    
  if 'TLTA' in (explanatoryVariablesToCalculate):
    # Calculate TLTA
    explanatoryDataFrame['TLTA'] = calculateTLTA(PRC=explanatoryDataFrame['PRC'], 
                                                 SHROUT=explanatoryDataFrame['SHROUT'],
                                                 CEQQ=explanatoryDataFrame['ceqq'],
                                                 ATQ=explanatoryDataFrame['atq'],
                                                 LTQ=explanatoryDataFrame['ltq']
                                                 )
    
  if 'TLMTA' in (explanatoryVariablesToCalculate):
    # Calculate TLTA
    explanatoryDataFrame['TLMTA'] = calculateTLMTA(PRC=explanatoryDataFrame['PRC'], 
                                                 SHROUT=explanatoryDataFrame['SHROUT'],
                                                 LTQ=explanatoryDataFrame['ltq']
                                                 )
    
  if 'EXRET' in (explanatoryVariablesToCalculate):
    # Calculate EXRET
    explanatoryDataFrame['EXRET'] = calculateEXRET(PRC=explanatoryDataFrame['PRC'], 
                                                   CFACPR=explanatoryDataFrame['CFACPR'],
                                                   VWRETDSP500=explanatoryDataFrame['vwretdSP500']
                                                 )
    
  if 'RSIZE' in (explanatoryVariablesToCalculate):
    # Calculate RSIZE
    explanatoryDataFrame['RSIZE'] = calculateRSIZE(PRC=explanatoryDataFrame['PRC'], 
                                                 SHROUT=explanatoryDataFrame['SHROUT'],
                                                 TOTVALSP500=explanatoryDataFrame['totvalSP500']
                                                 )
    
  if 'CASHMTA' in (explanatoryVariablesToCalculate):
    # Calculate CASHMTA
    explanatoryDataFrame['CASHMTA'] = calculateCASHMTA(PRC=explanatoryDataFrame['PRC'], 
                                                 SHROUT=explanatoryDataFrame['SHROUT'],
                                                 CHEQ=explanatoryDataFrame['cheq'],
                                                 LTQ=explanatoryDataFrame['ltq']
                                                 )


  if keepAllFeatures:
  	print('In')
  	explanatoryDataFrame = explanatoryDataFrame
  else:
  	print('out')
  	# Keep only selected columns
  	selectedColumns = identifyingColumns
  	selectedColumns.extend(['date_month'])
  	selectedColumns.extend(explanatoryVariablesToCalculate)
  	explanatoryDataFrame = explanatoryDataFrame[selectedColumns]

  return explanatoryDataFrame

def createXDataFrame(rawDataframes,
                     explanatoryVariablesToCalculate=['NITA', 
                                                      'NIMTA',
                                                      'TLTA',
                                                      'TLMTA',
                                                      'EXRET',
                                                      'RSIZE',
                                                      'CASHMTA',
                                                      'SIGMA'
                                                      ],
                     identifyingColumns = ['PERMNO', 'GVKEY','conm', 'cik'],
                     keepAllFeatures=False,
                     CRSP_COMPUSTAT_Accounting_features = ['atq', 'ceqq', 'cheq', 'ltq', 'niq'],
                     CRSP_COMPUSTAT_Identifying_features = ['GVKEY', 'conm', 'cik'],
                     CRSP_MONTHLY_features = ['PERMNO', 'date_month', 'PRC', 'SHROUT', 'CFACPR', 'RET'],
                     monthsToLagAccountingVariables=2
                     ):
  """
  Create X-Dataframe
  """
  # Load Raw Dataframes
  CRSP_COMPUSTAT_MERGED, CRSP_MONTHLY, CRSP_DAILY, SP500_MONTHLY = rawDataframes

  # Prepare Data
  CRSP_COMPUSTAT_MERGED = prepareCrspCompustatMergedData(CRSP_COMPUSTAT_MERGED, monthsToLagAccountingVariables)
  CRSP_MONTHLY = prepareCrspMonthlyData(CRSP_MONTHLY)
  CRSP_DAILY = prepareCrspDailyData(CRSP_DAILY)
  SP500_MONTHLY = prepareSP500Data(SP500_MONTHLY)

  # Merge Dataframes
  explanatoryDataFrame = mergeCrspCompustatMergedWithCrspMonthly(CRSP_COMPUSTAT_MERGED, 
                                                                 CRSP_MONTHLY,
                                                                 CRSP_COMPUSTAT_Accounting_features,
                                                                 CRSP_COMPUSTAT_Identifying_features,
                                                                 CRSP_MONTHLY_features
                                                                 )
  explanatoryDataFrame = mergeExplanatoryDataframeWithCrspDaily(explanatoryDataFrame, CRSP_DAILY)
  explanatoryDataFrame = mergeExplanatoryDataframeWithSP500Monthly(explanatoryDataFrame, SP500_MONTHLY)

  # Create Explanatory Variables
  explanatoryDataFrame = createCustomExplanatoryVariables(explanatoryDataFrame, 
                                                          explanatoryVariablesToCalculate,
                                                          identifyingColumns,
                                                          keepAllFeatures
                                                          )

  return explanatoryDataFrame