#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings
warnings.filterwarnings('ignore')


# In[2]:


import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime


# In[3]:


##getting the master data set
def loaddata():
    conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=nhc-azu-dw01;'
                     'Database=riskdw;'
                     'Trusted_Connection=yes;')
    df = pd.read_sql_query('SELECT * FROM riskdw.dbo.map_se_sub_accounts_data',conn)
    return df


# In[4]:


##Getting the CVAR and other data for the managers, Trading Level
def loaddata2():
    conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=nhc-azu-dw01;'
                     'Database=hedgemark;'
                     'Trusted_Connection=yes;')
    df = pd.read_sql_query('SELECT * FROM hedgemark.dbo.vw_daily_subaccount_w_returns',conn)
    return df


# In[5]:


##Getting the CVAR and other data for TAF
def loaddata3():
    conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=nhc-azu-dw01;'
                     'Database=hedgemark;'
                     'Trusted_Connection=yes;')
    df = pd.read_sql_query('SELECT * FROM hedgemark.dbo.vw_daily_MAP',conn)
    return df


# In[6]:


##Getting the vol_cont
#vol cont will be used for bss
def loaddata5():
    conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=nhc-azu-dw01;'
                     'Database=hedgemark;'
                     'Trusted_Connection=yes;')
    df = pd.read_sql_query('SELECT * FROM hedgemark.dbo.vw_daily_map_risk_contribution',conn)
    return df


# In[7]:


###loading the master dataset
df = loaddata()


# In[8]:


##saving the master as static copy
#compression_opts = dict(method='zip',
#                        archive_name='se_sub_accounts_data.csv')
#df.to_csv('se_sub_accounts_data.zip', index=False,
#          compression=compression_opts)


# In[9]:


###STEP 1 - Create _margin_ex_losses columns (3/23)


# In[10]:


##create the docmargin reference table (Step 1)
docmargin = pd.DataFrame()
docmargin['Doc Type'] = pd.Series(['FCM','ISDA', 'CDA','FXPB','ISDA EQ Swap','PB'])
docmargin['BSS'] = pd.Series([.1,.2,.2,.1,0,0])
docmargin['MSS'] = pd.Series([.15,.25,.25,.15,0,0])
docmargin['WSS'] = pd.Series([.2,.3,.3,.2,0,0])


# In[11]:


##dropping na values from adjusted_margin column (~88 values dropped)
df = df[df['adjusted_margin'].notna()].reset_index(drop=True)


# In[12]:


###CREATING NEW COLUMNS (Step 1)
df['bss_margin_ex_losses'] = np.nan
df['wss_margin_ex_losses'] = np.nan
df['mss_margin_ex_losses'] = np.nan


# In[13]:


#float(docmargin[docmargin['Doc Type']=='FXPB']['BSS'])
#STEP 1 (Margin Stress Columns)

for i in range(len(df)):
    doc = df['Broker Type'][i]

    df['bss_margin_ex_losses'][i] = max(0,df['adjusted_margin'][i]*(1+float(docmargin[docmargin['Doc Type']==doc]['BSS'])))
    df['wss_margin_ex_losses'][i] = max(0,df['adjusted_margin'][i]*(1+float(docmargin[docmargin['Doc Type']==doc]['WSS'])))
    df['mss_margin_ex_losses'][i] = max(0,df['adjusted_margin'][i]*(1+float(docmargin[docmargin['Doc Type']==doc]['MSS'])))


# In[14]:


#saving the 3 created columns
compression_opts = dict(method='zip', archive_name='step1.csv')
df.to_csv('step1.zip', index=False,
          compression=compression_opts)


# In[15]:


###Aggregating the _margin_ex_losses by manager and day
df['Aggregated bss_margin_ex_losses'] = df.groupby(['Date','Fund'])['bss_margin_ex_losses'].transform('sum')
df['Aggregated mss_margin_ex_losses'] = df.groupby(['Date','Fund'])['wss_margin_ex_losses'].transform('sum')
df['Aggregated wss_margin_ex_losses'] = df.groupby(['Date','Fund'])['mss_margin_ex_losses'].transform('sum')


# In[16]:


###aggregating the other columns
cols = ['Total Equity','Total Margin Requirement', 'Excess/Deficit', 'Stand Alone Requirement',
       'Top Level Requirement', 'adjusted_margin']
for k in cols:
    df['Aggregated '+k] = df.groupby(['Date','Fund'])[k].transform('sum')


# In[17]:


##create new dataframe with only mgr/day rows (aggregated on broker type)
dfnew = df.drop_duplicates(subset=['Date', 'Fund']).reset_index(drop=True)
dfnew = dfnew[['Date', 'Fund', 'Currency','Aggregated bss_margin_ex_losses', 'Aggregated mss_margin_ex_losses',
       'Aggregated wss_margin_ex_losses', 'Aggregated Total Equity',
       'Aggregated Total Margin Requirement', 'Aggregated Excess/Deficit',
       'Aggregated Stand Alone Requirement',
       'Aggregated Top Level Requirement', 'Aggregated adjusted_margin']]


# In[18]:


#saving the aggregated dataframe
compression_opts = dict(method='zip',
                        archive_name='se_sub_accounts_data.csv')
dfnew.to_csv('aggregated.zip', index=False,
          compression=compression_opts)


# In[19]:


###END STEP 1


# In[ ]:





# In[20]:


###STEP 2 (part1):
###Pull CVaR data for TAF and individual Managers. Pull trading level data. Pull Vol cont data. 
###Create bss/mss/wss columns (6/23)
###Create (b/m/w)ss_margin columns (9/23)
###Create Safety column, safety_dollar column, Supporting Equity 1/2, (b/w/m)ss_dollar columns (16/23)


# In[21]:


#load the data
df2 = loaddata2()
df4 = loaddata3()
volcont= loaddata5()


# In[22]:


#compression_opts = dict(method='zip',
#                        archive_name='vw_daily_subaccount_w_returns.csv')
#df2.to_csv('vw_daily_subaccount_w_returns.zip', index=False, compression=compression_opts)


# In[23]:


#subset individual manager the data to include only relevant columns
df3 = df2[['context_date', 'fund_id_hedgemark','fund_name_hedgemark','trading_level_variable','day_cvar_99_pct_94_decay',
           'day_cvar_99_pct_no_decay']]


# In[24]:


###Aggregating the metrics by manager and day, making it absolute
df3['Aggregated trading_level_variable'] = abs(df3.groupby(['context_date','fund_name_hedgemark'])['trading_level_variable'].transform('sum'))
df3['Aggregated day_cvar_99_94'] = abs(df3.groupby(['context_date','fund_name_hedgemark'])['day_cvar_99_pct_94_decay'].transform('sum'))
df3['Aggregated day_cvar_99_no'] = abs(df3.groupby(['context_date','fund_name_hedgemark'])['day_cvar_99_pct_no_decay'].transform('sum'))

### getting CVaR as pct of trading level variable, make it absolute value
df3['day_cvar_99_94_%'] = abs(df3['Aggregated day_cvar_99_94']/df3['Aggregated trading_level_variable'])
df3['day_cvar_99_no_%'] = abs(df3['Aggregated day_cvar_99_no']/df3['Aggregated trading_level_variable'])


# In[ ]:





# In[25]:


#subset the taf data to only include taf
tafdata = df4[df4['fund_name_hedgemark']=='TAF'].reset_index()


# In[26]:


#clean the volcont data, make the names match up with other two datasets
volcont2 = volcont[volcont['fund_name_hedgemark'].str.contains("TAF")].reset_index(drop=True)
volcont2['NHC Names'] = 'TAF_' + volcont2['new_holland_account'] 

for i in range(len(volcont2)):
    if volcont2['NHC Names'][i] == 'TAF_OVATA':
        volcont2['NHC Names'][i] = 'TAF_Ovata'
    if volcont2['NHC Names'][i] == 'TAF_Gamma Q':
        volcont2['NHC Names'][i] = 'TAF_GammaQ'
    if volcont2['NHC Names'][i] == 'TAF_CENTERLINE':
        volcont2['NHC Names'][i] = 'TAF_Centerline'
    if volcont2['NHC Names'][i] == 'TAF_Tri Locum':
        volcont2['NHC Names'][i] = 'TAF_TriLocum'    
    if volcont2['NHC Names'][i] == 'TAF_Sand Grove SIP':
        volcont2['NHC Names'][i] = 'TAF_Sand Grove Arb'  
    if volcont2['NHC Names'][i] == 'TAF_PERISCOPE':
        volcont2['NHC Names'][i] = 'TAF_Periscope'  


# In[27]:


##create the columns
dfnew['safety'] = 0.05
dfnew['bss'] = np.nan
dfnew['wss'] = np.nan
dfnew['mss'] = np.nan
dfnew['bss_margin'] = np.nan
dfnew['wss_margin'] = np.nan
dfnew['mss_margin'] = np.nan


# In[28]:


#create wss_margin and wss
for i in range(len(dfnew)):
    temp = df3[df3['fund_name_hedgemark']==dfnew['Fund'][i]]
    temp2 = temp[temp['context_date']==dfnew['Date'][i]].reset_index()
    if len(temp2)>0:
        dfnew['wss'][i] = 0.5*np.sqrt(63)*(temp2['day_cvar_99_94_%']+
                                        temp2['day_cvar_99_no_%'])[0]
        dfnew['wss_margin'][i] = dfnew['Aggregated wss_margin_ex_losses'][i]*(1-dfnew['wss'][i])


# In[29]:


##make vol cont be at minimum 0, not negative
volcont2['volcont99'] = np.nan
for i in range(len(volcont2)):
    volcont2['volcont99'][i] = max(0,volcont2['day_cvar_99_pct_historical_2yr_contribution'][i]/100)


# In[30]:


##create TAF cvar as a percent
tafdata['day_cvar_99_no%'] = tafdata['day_cvar_99_pct_no_decay']/tafdata['market_value_admin']
tafdata['day_cvar_99_94%'] = tafdata['day_cvar_99_pct_94_decay']/tafdata['market_value_admin']


# In[31]:


#create bss_margin and bss, mss, mss_margin
for i in range(len(dfnew)):
    temp = volcont2.loc[(volcont2['context_date'] == dfnew['Date'][i]) & 
                        (volcont2['NHC Names'] == dfnew['Fund'][i])]['volcont99'].reset_index(drop=True)
    temp2 =  tafdata.loc[(tafdata['context_date'] == dfnew['Date'][i])]['day_cvar_99_no%'].reset_index(drop=True)
    temp3 =  tafdata.loc[(tafdata['context_date'] == dfnew['Date'][i])]['day_cvar_99_94%'].reset_index(drop=True)
    if (len(temp)>0 and len(temp2)>0 and len(temp3)>0):
        dfnew['bss'][i] = max(0, 0.5*np.sqrt(63)*temp[0]*(abs(temp3[0])+abs(temp2[0])))
        dfnew['mss'][i] = 0.5*(dfnew['wss'][i]+dfnew['bss'][i])
        dfnew['bss_margin'][i] = dfnew['Aggregated bss_margin_ex_losses'][i]*(1-dfnew['bss'][i])
        dfnew['mss_margin'][i] = dfnew['Aggregated mss_margin_ex_losses'][i]*(1-dfnew['mss'][i])


# In[ ]:





# In[32]:


#compression_opts = dict(method='zip',
#                        archive_name='dfwithss.csv')
#df.to_csv('dfwithss.zip', index=False, compression=compression_opts)


# In[33]:


##Adding in trading_level_variable
dfnew['safety_dollar'] = np.nan
dfnew['trading_level_variable'] = np.nan

for i in range(len(dfnew)):
    temp = df3.loc[(df3['context_date'] == dfnew['Date'][i]) & 
                        (df3['fund_name_hedgemark'] == dfnew['Fund'][i])]['Aggregated trading_level_variable'].reset_index(drop=True)
    if len(temp)>0:
        dfnew['safety_dollar'][i] = 0.05*temp[0]
        dfnew['trading_level_variable'][i] = temp[0]


# In[34]:


##creating bss/wss/mss_dollar
dfnew['bss_dollar'] = dfnew['bss']*dfnew['trading_level_variable']
dfnew['wss_dollar'] = dfnew['wss']*dfnew['trading_level_variable']
dfnew['mss_dollar'] = dfnew['mss']*dfnew['trading_level_variable']
#creating supporting equity 1, supporting equity 2
dfnew['Supporting Equity 1'] = dfnew['wss_margin'] + dfnew['wss_dollar'] + dfnew['safety_dollar']
dfnew['Supporting Equity 2'] = dfnew['wss_margin'] + dfnew['wss_dollar'] 


# In[35]:


compression_opts = dict(method='zip',
                        archive_name='inspect.csv')
dfnew.to_csv('inspect.zip', index=False, compression=compression_opts)


# In[ ]:





# In[36]:


###STEP 2 (part2):
###add in multiple frequency of returns, net and gross exposures


# In[37]:


df5 = df2[['context_date','fund_name_hedgemark','trading_level_variable',
           '10_yr_equiv_long_notional','10_yr_equiv_short_notional','10_yr_equiv_gross_notional','10_yr_equiv_net_notional']]


# In[38]:


###Aggregating the metrics by manager and day
df5['Aggregated trading_level_variable'] = df5.groupby(['context_date','fund_name_hedgemark'])['trading_level_variable'].transform('sum')
df5['Aggregated 10_yr_equiv_long_notional'] = df5.groupby(['context_date','fund_name_hedgemark'])['10_yr_equiv_long_notional'].transform('sum')
df5['Aggregated 10_yr_equiv_short_notional'] = df5.groupby(['context_date','fund_name_hedgemark'])['10_yr_equiv_short_notional'].transform('sum')
df5['Aggregated 10_yr_equiv_gross_notional'] = df5.groupby(['context_date','fund_name_hedgemark'])['10_yr_equiv_gross_notional'].transform('sum')
df5['Aggregated 10_yr_equiv_net_notional'] = df5.groupby(['context_date','fund_name_hedgemark'])['10_yr_equiv_net_notional'].transform('sum')


# In[39]:


##Adding in notionals
dfnew['Aggregated trading_level_variable'] = np.nan
dfnew['Aggregated 10_yr_equiv_long_notional'] = np.nan
dfnew['Aggregated 10_yr_equiv_short_notional'] = np.nan
dfnew['Aggregated 10_yr_equiv_gross_notional'] = np.nan
dfnew['Aggregated 10_yr_equiv_net_notional'] = np.nan

cols = ['Aggregated trading_level_variable','Aggregated 10_yr_equiv_long_notional','Aggregated 10_yr_equiv_short_notional',
       'Aggregated 10_yr_equiv_gross_notional','Aggregated 10_yr_equiv_net_notional']

for i in range(len(dfnew)):
    for j in cols:
        temp = df5.loc[(df5['context_date'] == dfnew['Date'][i]) & 
                            (df5['fund_name_hedgemark'] == dfnew['Fund'][i])][j].reset_index(drop=True)
        if len(temp)>0:
            dfnew[j][i] = temp[0]


# In[40]:


compression_opts = dict(method='zip',
                        archive_name='step2.csv')
dfnew.to_csv('step2.zip', index=False, compression=compression_opts)


# In[41]:


###Adding in Leverage Ratio = TL/Supporting Equity 2
dfnew['Leverage Ratio'] = dfnew['trading_level_variable']/dfnew['Supporting Equity 2']


# In[ ]:





# In[42]:


###Cleaning up the dataframe dfnew
dfn2 = dfnew[['Date', 'Fund','Aggregated bss_margin_ex_losses','Aggregated mss_margin_ex_losses',
           'Aggregated wss_margin_ex_losses','Aggregated Total Equity','Aggregated Total Margin Requirement',
              'Aggregated Excess/Deficit','Aggregated Stand Alone Requirement',
       'Aggregated Top Level Requirement', 'Aggregated adjusted_margin',
       'safety', 'bss', 'wss', 'mss', 'bss_margin', 'wss_margin', 'mss_margin',
       'safety_dollar', 'trading_level_variable', 'bss_dollar', 'wss_dollar',
       'mss_dollar', 'Supporting Equity 1', 'Supporting Equity 2',
       'Aggregated 10_yr_equiv_long_notional',
       'Aggregated 10_yr_equiv_short_notional',
       'Aggregated 10_yr_equiv_gross_notional',
       'Aggregated 10_yr_equiv_net_notional', 'Leverage Ratio']]
dfn2.columns = [ dfn2.columns[i].replace('Aggregated ','') for i in range(len(dfn2.columns)) ]


# In[43]:


for i in range(len(dfn2)):
    dfn2['Date'][i] = pd.to_datetime(dfn2['Date'][i])


# In[44]:


##sort by date
dfn2 = dfn2.sort_values(by='Date').reset_index(drop=True)


# In[45]:


##do one fund at a time, created SE 1, SE 2 smoothed (moving average 21)
##adjusted margin, moving average 5

dfn2['Supporting Equity Smoothed 1'] = np.nan
dfn2['Supporting Equity Smoothed 2'] = np.nan
dfn2['adj_margin_5day_MA'] = np.nan
dfn2['adj_margin_5day_MA_1day_lag'] = np.nan

funds = list(set(dfn2['Fund']))

#i cycles through the funds, j cycles through the rows in each fund

for i in funds:
    temp = dfn2[dfn2['Fund'] == i].reset_index(drop=True)
    for j in range(len(temp)):
        b = max(j-21,0)
        dfn2.loc[(dfn2['Date'] == temp['Date'][j]) & 
                   (dfn2['Fund'] == i),'Supporting Equity Smoothed 1'] = np.average(temp['Supporting Equity 1'][b:j])
        dfn2.loc[(dfn2['Date'] == temp['Date'][j]) & 
                   (dfn2['Fund'] == i),'Supporting Equity Smoothed 2'] = np.average(temp['Supporting Equity 2'][b:j])
        
        ###putting in the 5 day MA
        c = max(j-5,0)
        dfn2.loc[(dfn2['Date'] == temp['Date'][j]) & 
                   (dfn2['Fund'] == i),'adj_margin_5day_MA'] = np.average(temp['adjusted_margin'][c:j])
        
        ###putting in the 5 day MA with 1 day lag (if it's the first day of the fund, keep as nan)
        if j > 0:
            dfn2.loc[(dfn2['Date'] == temp['Date'][j]) & 
                   (dfn2['Fund'] == i),'adj_margin_5day_MA_1day_lag'] = float(dfn2.loc[(dfn2['Date'] == temp['Date'][j-1]) & 
                   (dfn2['Fund'] == i)]['adj_margin_5day_MA'])


# In[46]:


##margin one month back
dfn2['margin_1_month_back'] = np.nan
dfn2['monthly_change'] = np.nan

#i cycles through the funds, j cycles through the rows in each fund

for i in funds:
    temp = dfn2[dfn2['Fund'] == i].reset_index(drop=True)
    for j in range(len(temp)):
        
        ###putting in the 1 month back (21 days back)
        b = max(j-21,0)
        ###(if it's not been 21 days of the fund record, keep as nan)
        if j > 20:
            dfn2.loc[(dfn2['Date'] == temp['Date'][j]) & 
                   (dfn2['Fund'] == i),'margin_1_month_back'] = float(dfn2.loc[(dfn2['Date'] == temp['Date'][j-21]) & 
                   (dfn2['Fund'] == i)]['adjusted_margin'])

dfn2['monthly_change'] = dfn2['adjusted_margin']/dfn2['margin_1_month_back'] - 1


# In[47]:


compression_opts = dict(method='zip',
                        archive_name='final.csv')
dfn2.to_csv('final.zip', index=False, compression=compression_opts)


# In[48]:


dfn2.columns

