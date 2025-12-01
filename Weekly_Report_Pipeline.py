
import csv

import pandas as pd
import re


# Read the CSV files
df1 = pd.read_csv('NATIONWIDE REPORT_GB.csv')
df2 = pd.read_csv('NATIONWIDE_RO_NOV.csv')
df3 = pd.read_csv('Agents_RM.csv')
df4 = pd.read_csv('rm_branch_region.csv')
df5 = pd.read_csv('Agents_channel.csv')


# Rename RM NAME to RO NAME only if RM NAME exists
if 'RM NAME' in df4.columns:
    df4 = df4.rename(columns={'RM NAME': 'RO NAME'})

# df4['RO NAME'] = df4['RO NAME'].str.strip().str.upper()
# if 'BRANCH' in df4.columns:
#     df4['BRANCH'] = df4['BRANCH'].str.strip().str.upper()

# 1️⃣ Read your existing RM branch file
new_rms = pd.DataFrame({
    'RO NAME': ['UDOCHUKWU NWOSU', 'ESTHER AIYEKOMOGBON', 'OKANKE EZE', 'CHIJIOKE EKWONYE', 'OLUBUKOLA ADEMOLA','IJEOMA ONYEJIUWA','LAWREEN ARCHIBONG','AHMED OYEGUNLE','PAUL ADARAMOLA','TOCHUKWU DIKE','CHIMEZIE IJEZIE'],
    'BRANCH': ['PORT HARCOURT', 'CORPORATE OFFICE', 'CORPORATE OFFICE', 'CORPORATE OFFICE', 'BANC', 'BANC', 'CALABAR', 'UYO','ABUJA','GLOBAL CLIENTS','CORPORATE OFFICE'],
    # 'REGION': ['SOUTH', 'LAGOS', 'LAGOS', 'LAGOS', 'GLOBAL CLIENTS', 'LAGOS', 'LAGOS', 'SOUTH', 'SOUTH', 'ABUJA']
})

#update Channel with the new Agent from the channel list
new_Agent_channel = pd.DataFrame({
    'BROKERS/AGENTS': ['GERMANE TRUST INSURANCE BROKER', 'SANLAM INSURANCE LIMITED','IWUALA NDIDIAMAKA CYNTHIA','OGUNTOYINBO JEDIDAH OLUWATOSIN','DEJI-ADEBISI OZIE BRIDGET','ILAWOLE BUSAYO MICHAEL'],
    'CHANNEL': ['BROKER','BROKER','AGENCY','BROKER','BROKER','AGENCY']
})
#Append the new Agent_channel
df5 = pd.concat([df5, new_Agent_channel], ignore_index=True)
df5 = df5.drop_duplicates(subset=['BROKERS/AGENTS'])

# 3️⃣ Append the new rows
df4 = pd.concat([df4, new_rms], ignore_index=True)
df4 = df4.drop_duplicates(subset=['RO NAME'])  

# 2️⃣ Update the CHANNEL for the specific agent
# Create a dictionary of Broker/Agent → Channel
channel_updates = {
    'AKPOKODJE CHIDERA NTAKARE': 'AGENCY',
    'SEGUN SAINT-WONDER': 'AGENCY',
    'EDO ROBERTS DANIEL': 'AGENCY',
    'INTERNATIONAL ENERGY INSURANCE COMPANY LIMITED': 'RETAIL',
    'FIRST STANDARD INSURANCE BROKER': 'RETAIL',
    'BISIRIYU ABIODUN NAFISAT': 'AGENCY',
    'AFFINITY INSURANCE BROKERS LTD': 'BROKER',
    'OLAYINKA IBUKUN KAYODE': 'AGENCY',
    'AFOLABI VICTOR OLAYEMI': 'AGENCY',
    'ADERUPATAN OMONIYI GABRIEL': 'AGENCY',
    'SANNI ROMOKE BUSHRAH': 'AGENCY',
    'MBACHU LORRETA NKECHINYERE': 'AGENCY',
    'AYODEJI ONI': 'AGENCY',
    'OKODUWA SUNDAY': 'AGENCY',
    'AYOADE GIDEON SUNDAY': 'AGENCY',
    'JOSEPH-KEVWE OLUWAFUNMILAYO LYDIA': 'AGENCY',
    'MULTILINE INSURANCE BROKERS LTD': 'BROKER',
    'AKINLALU OLUGBENGA SAMUEL': 'AGENCY',
    'EGUABOR OFURE ANGELA': 'AGENCY'
}

# Standardize names in df5
df5['BROKERS/AGENTS'] = df5['BROKERS/AGENTS'].str.strip().str.upper()

# Apply the updates
for name, channel in channel_updates.items():
    df5.loc[df5['BROKERS/AGENTS'] == name.upper(), 'CHANNEL'] = channel.upper()




#Clean column names by stripping whitespace
for df in [df1, df2, df3, df4, df5]:
    df.columns = df.columns.str.strip()

# Rename columns for consistency
df2= df2.rename(columns={'DEBIT NOTE No': 'DEBIT NOTE NUMBER'})
df4 = df4.rename(columns={'RM NAME': 'RO NAME'})
df3 = df3.rename(columns={'AGENCY': 'AGENT','RM NAME': 'RM NAME'})
df5 = df5.rename(columns= {'BROKERS/AGENTS': 'AGENT'})

#Drop All duplicates base on debit note number
df2 = df2.drop_duplicates(subset=['DEBIT NOTE NUMBER'])
df3 = df3.drop_duplicates(subset=['AGENT'])
df4 = df4.drop_duplicates(subset=['RO NAME'])
df5 = df5.drop_duplicates(subset=['AGENT'])




# Merge the dataframes(Weekly_file) on 'POLICY NUMBER'
merged_df = pd.merge(df1, df2[['DEBIT NOTE NUMBER', 'RO NAME']], on='DEBIT NOTE NUMBER', how='left')
#merge RO Name on Branch and Region
merged_df7 = pd.merge(merged_df, df4[['RO NAME', 'BRANCH', 'REGION']], on='RO NAME', how='left')
#merge Agent on channel
merged_df8 = pd.merge(merged_df7, df5[['AGENT', 'CHANNEL']], on= 'AGENT', how= 'left')
#merge Agent with RM Name
df3 = df3.rename(columns={'RM NAME': 'RM NAME FROM AGENT'})
merged_df8 = pd.merge(merged_df8, df3[['AGENT', 'RM NAME FROM AGENT']], on= 'AGENT', how= 'left')

#Replace empty space from RO Name with NaN for consistency
merged_df8['RO NAME'] = merged_df8['RO NAME'].replace('', pd.NA)

#Replace RO Name with the Agent RM Name file if its blank or Nan
merged_df8['RO NAME'] = merged_df8['RO NAME'].combine_first(merged_df8['RM NAME FROM AGENT'])


#merge the filled RM NAME FROM AGENT with the Branch and Region file
merged_df8 = pd.merge(merged_df8.drop(columns=['BRANCH', 'REGION'], errors= 'ignore'), df4[['RO NAME', 'BRANCH', 'REGION']], on='RO NAME', how='left')


#Clean up
merged_df8 = merged_df8.drop(columns=['RM NAME FROM AGENT'], errors= 'ignore')
merged_df8 = merged_df8.drop_duplicates()


merged_df8.to_csv('Weekly_report_40_check5.csv', index=False)

#if the RO Name is still Nan or empty fill the BRANCH rows that are empty with the Branch_x and map the region with the rm branch_region

df6 = pd.read_csv('Weekly_report_40_check5.csv')
df6.columns = df6.columns.str.strip()

#Replace empty space from RO Name with NaN for consistency
df6['RO NAME'] = df6['RO NAME'].replace('', pd.NA)
#Overrite BRANCH column with BRANCH_x values when RO NAME is NaN or empty
if 'BRANCH_x' in df6.columns:
    df6.loc[df6['RO NAME'].isna(), 'BRANCH'] = df6.loc[df6['RO NAME'].isna(), 'BRANCH_x']
else:
    print("Column 'BRANCH_x' does not exist in the dataframe.")  

#Fix deduplication issue by dropping REGION column before remapping
df4 = df4.drop_duplicates(subset=['BRANCH'])

#Drop Region before remapping
df6 = df6.drop(columns=['REGION'], errors= 'ignore')


#Map REGION based on BRANCH using rm_branch_region file
df6 = pd.merge(df6, df4[['BRANCH', 'REGION']], on='BRANCH', how='left')

#Drop duplicates that may have arisen during the merge
df6 = df6.drop_duplicates(subset=['DEBIT NOTE NUMBER', 'RO NAME', 'BRANCH', 'REGION', 'AGENT', 'CHANNEL'])

#Creating column  for specialty to check the column if [GRP CLASS] == 'AVIATION', 'ENERGY', 'MARINE' = 'SR' else 'NSR' or column [SUB CLASS] == 'CONSOLIDATED ENERGY' = 'SR' else 'NSR'
df6['SPECIALTY'] = df6.apply(lambda row: 'SR' if (row['GRP CLASS'] == 'AVIATION' or row['GRP CLASS'] == 'ENERGY' or row['SUB CLASS'] == 'CONSOLIDATED ENERGY POLICY') else 'NSR', axis=1)
#creating column for channel to check IF([@CHANNEL]="ALT", "RETAIL", IF([@CHANNEL]="AGENCY", "RETAIL", IF([@CHANNEL]= "BANC","RETAIL","B2B")))
df6['CHANNEL TYPE'] = df6['CHANNEL'].apply(lambda x: 'RETAIL' if x in ['ALT', 'AGENCY', 'BANC'] else 'B2B')
#creating column for Transaction type to check IF([@[TRANS TYPE]]="ENDORSEMENT", "RENEWAL", IF([@[TRANS TYPE]]="RENEWAL", "RENEWAL", "NEW BUSINESS"))
df6['TRANSACTION TYPE'] = df6['TRANS TYPE'].apply(lambda x: 'RENEWAL' if x in ['ENDORSEMENT', 'RENEWAL'] else 'NEW BUSINESS')
#reorder this columns from the file like this DEBIT NOTE NUMBER , RO NAME ,AGENT, LAC PREMIUM , COMM ,NET PREMIUM ,TRASACTION DATE (AUTHORISATION),START DATE	,END DATE,DEBIT NOTE NUMBER,POLICY NUMBER,INSURED NAME,TRANS TYPE,CLIENT TYPE,AUTHORISER,MAKE READY BY,	PREPARER,PAYMENT MODE REFERENCE NUMBER	,DATE ON PAYMENT  REFERENCE,CURR,GRP CLASS,SUB CLASS,MODE OF PYMT,LAC LEAD?,LEADER,100% SUM INSURED , 100% PREMIUM , LAC PROP. (%) ,LAC SUM INSURED ,AGENT CODE,BRANCH_x,TRANSACTION DATE (MAKE READY),	TRANS. YEAR,RO SHARE,CLASS,PRM WGT,CHANNEL,SUB - CHANNEL,CHANNEL TYPE,BRANCH,REGION,TRANSACTION TYPE, SPECIALTY If any of the columns are missing in the df6, the column should be left blank but added  in the reordering process.
# desired_order = ['DEBIT NOTE NUMBER', 'RO NAME', 'AGENT', 'LAC PREMIUM', 'COMM', 'NET PREMIUM', 'TRASACTION DATE (AUTHORISATION)', 'START DATE', 'END DATE', 'DEBIT NOTE NUMBER', 'POLICY NUMBER', 'OLD POLICY NUMBER','INSURED NAME', 'TRANS TYPE', 'CLIENT TYPE', 'AUTHORISER', 'MAKE READY BY', 'PREPARER', 'PAYMENT MODE REFERENCE NUMBER', 'DATE ON PAYMENT  REFERENCE', 'CURR', 'GRP CLASS', 'SUB CLASS', 'MODE OF PYMT', 'LAC LEAD?', 'LEADER', '100% SUM INSURED', '100% PREMIUM', 'LAC PROP. (%)', 'LAC SUM INSURED', 'AGENT CODE', 'BRANCH_x', 'TRANSACTION DATE (MAKE READY)', 'TRANS. YEAR', 'RO SHARE', 'GRP CLASS', 'PRM WGT', 'CHANNEL', 'SUB - CHANNEL', 'CHANNEL TYPE', 'BRANCH', 'REGION', 'TRANSACTION TYPE', 'SPECIALTY']
desired_order = ['DEBIT NOTE NUMBER', 'AGENT', 'LAC PREMIUM', 'COMM', 'TRASACTION DATE (AUTHORISATION)', 'START DATE', 'END DATE', 'DEBIT NOTE NUMBER', 'POLICY NUMBER','INSURED NAME', 'TRANS TYPE', 'CLIENT TYPE', 'AUTHORISER', 'MAKE READY BY', 'PAYMENT MODE REFERENCE NUMBER', 'DATE ON PAYMENT  REFERENCE', 'RO NAME','MONTH', 'GRP CLASS', 'SUB CLASS', 'SPECIALTY', 'BRANCH', 'REGION','CHANNEL','COMMENT' ]
for col in desired_order:
    if col not in df6.columns:
        df6[col] = ""

df6 = df6[desired_order]

# df6.to_csv('Weekly_report_40_check9.csv', index=False)
df6.to_csv('Nationwide_November_report.csv', index=False)


#Final verification

print(merged_df8.head())
print("\n=== Final Verification Summary ===")
print(f"Total rows: {len(df6)}")
print(f"Unique Debit Note Numbers: {df6['DEBIT NOTE NUMBER'].nunique()}")
print(f"Rows with missing RO NAME: {df6['RO NAME'].isna().sum()}")
print(f"Rows with missing CHANNEL: {df6['CHANNEL'].isna().sum()}")
print(f"Rows with missing REGION: {df6['REGION'].isna().sum()}")
print(f"Rows with missing BRANCH: {df6['BRANCH'].isna().sum()}")
print(f"Rows with missing SPECIALTY: {df6['SPECIALTY'].isna().sum()}")
