"""
Wrapper that Formats Bloomberg Bankruptcy Data
01-01-2021
"""

import pandas as pd
import re


def companyIDExtract(securityID):
  """
  Extract Company ID from Security ID Field
  """
  companyID = re.split(" US Equity", securityID)[0]
  return companyID

def publicCompanyIdentify(companyID):
  """
  Determine if a company is public (1) or private (0) based on Company ID.
  A company is determined to be public if its Company ID begins with a letter
  and private otherwise.
  """
  if re.match("[a-zA-Z]", companyID[0]):
    publicCompany = 1
  else:
    publicCompany = 0
  return publicCompany

def filingTypeExtract(filingType):
  """
  Extract Filing Type from Filing Type Field
  """
  filingType = re.split("Filing Type: ", filingType)[1]
  return filingType

def companyNameExtract(companyName):
  """
  Extract Company name from companyName field
  """
  companyName = re.split("Name: ", companyName)[1]
  return companyName

def formatBloombergBankruptcyData(bloombergBankruptcyData, filterPublicCompanies=False):
  """
  Format Bloomberg Bankruptcy Data.

  Returns formatted Bloomberg Bankruptcy Data Pandas Dataframe
  """
  # Copy Dataframe so as not to alter original
  bloombergBankruptcyDataCopy = bloombergBankruptcyData.copy()

  # Extract Company ID
  bloombergBankruptcyDataCopy['companyID'] = bloombergBankruptcyDataCopy['Security ID'].map(companyIDExtract)

  # Determine Whether Company is Public or Private
  bloombergBankruptcyDataCopy['isPublic'] = bloombergBankruptcyDataCopy['companyID'].map(publicCompanyIdentify)

  # Extract Filing Type
  bloombergBankruptcyDataCopy['filingType'] = bloombergBankruptcyDataCopy['filingType'].map(filingTypeExtract)

  # Extract Company Name
  bloombergBankruptcyDataCopy['companyName'] = bloombergBankruptcyDataCopy['companyName'].map(companyNameExtract)

  # Convert Announce/Declared Date to a pandas datetime object
  bloombergBankruptcyDataCopy['Announce/Declared Date'] = pd.to_datetime(bloombergBankruptcyDataCopy['Announce/Declared Date'])

  # Convert Effective Date to a pandas datetime object
  bloombergBankruptcyDataCopy['Effective Date'] = pd.to_datetime(bloombergBankruptcyDataCopy['Effective Date'])

  # Filter only Public Companies
  if filterPublicCompanies:
    bloombergBankruptcyDataCopy = bloombergBankruptcyDataCopy[bloombergBankruptcyDataCopy['isPublic']==1]

  # Return Only Desired Fields
  return bloombergBankruptcyDataCopy[['companyID', 
                                      'companyName', 
                                      'Announce/Declared Date',
                                      'Effective Date',
                                      'filingType',
                                      'isPublic'
                                      ]]




