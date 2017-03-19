from datetime import datetime
from dateutil import relativedelta
import pickle

TO_DATE = datetime.now()
# TO_DATE = datetime(2009, 1, 1)
FROM_DATE = TO_DATE - relativedelta.relativedelta(months=12)

# # Load data for ASX securitues

# In[2]:

import pandas as pd
import pandas_datareader.data as web

df_asx300secs = pd.read_csv("ASXListedCompanies.csv")

# # Load pricing data from Yahoo

# In[3]:

pricing_panel = None

secs = df_asx300secs.Code.values

# Load from Yahoo or from pickle?
if False:
    with open("stock_prices.pickle", 'rb') as file:
        pricing_panel = pickle.load(file)
else:
    num = 10

    for index in range(0, len(secs), num):

        codes = ["%s.AX" % x for x in secs[index:index + num]]

        print "Loading data for %s" % codes

        data = web.DataReader(codes, 'yahoo', FROM_DATE, TO_DATE)

        if pricing_panel is None:
            pricing_panel = data
        else:
            pricing_panel = pd.concat([pricing_panel, data], axis=2)

    pricing_panel = pricing_panel.dropna(axis=2, how="all")

    with open('stock_prices.pickle', 'wb') as handle:
        pickle.dump(pricing_panel, handle)

    print "Done"

# In[4]:

pricing_data = {}
for sec_val in pricing_panel.minor_axis:
    sec = sec_val[0:-3]
    pricing_data[sec] = pricing_panel[:, :, sec_val]

# # Momentum calculations

# In[5]:

from talib import SMA
import numpy as np

MY_SHORT_MAV_TIME_PERIOD = 12
MY_MAV_TIME_PERIOD = 64

for sec in pricing_data.keys():
    pricing_data[sec]["MY_MAV"] = SMA(pricing_data[sec]["Close"].values, timeperiod=MY_MAV_TIME_PERIOD)
    pricing_data[sec]["MY_SHORT_MAV"] = SMA(pricing_data[sec]["Close"].values, timeperiod=MY_SHORT_MAV_TIME_PERIOD)
    pricing_data[sec]["MY_RSI"] = pricing_data[sec]["MY_SHORT_MAV"] - pricing_data[sec]["MY_MAV"]
    pricing_data[sec]["MY_RSI_RANK"] = pricing_data[sec]["MY_RSI"].rank(pct=True, method='average').round(2) - 0.01
    pricing_data[sec]["Days_Over_Under"] = np.where(pricing_data[sec]["MY_SHORT_MAV"] > pricing_data[sec]["MY_MAV"], 1,
                                                    -1)
    y = pricing_data[sec]["Days_Over_Under"]
    pricing_data[sec]["Days"] = y * (y.groupby((y != y.shift(1)).cumsum()).cumcount() + 1)
    pricing_data[sec]["Days_x_Ratio"] = ((pricing_data[sec]["Days"] * pricing_data[sec]["MY_RSI_RANK"]) / 50).round(
        0) * 50
    pricing_data[sec]["Rounded_Days"] = (pricing_data[sec]["Days"] / 10).round(0) * 10

# In[6]:

import pickle
from IPython.display import HTML

pd.set_option('display.max_colwidth', -1)

columns = []
columns.extend(df_asx300secs.columns)
columns.extend(["URL"])
columns.extend(pricing_data.itervalues().next().columns)

winners_vs_20 = pd.DataFrame(data=None, index=pricing_data.keys(), columns=columns)

for sec in pricing_data.keys():
    # Get the last row of the pricing data
    data = pricing_data[sec][-1:]
    winners_vs_20.loc[sec] = data.iloc[0]

    # Copy company details
    company = df_asx300secs.ix[df_asx300secs.Code == sec].to_dict("list")
    link = '<a href="https://au.finance.yahoo.com/echarts?s={0}.AX" target="_blank">{0}</a>'.format(sec)
    winners_vs_20.loc[sec]["URL"] = link
    for col in company.keys():
        winners_vs_20.loc[sec][col] = company[col][0]

# In[7]:

sorted_winners1 = winners_vs_20.sort_values(by=["MY_RSI_RANK", "Days_x_Ratio"], ascending=False)

sorted_winners = sorted_winners1[
    #   (sorted_winners1["Volume"] > 500000) &
    (sorted_winners1["Volume"] > 1000000) &
    (sorted_winners1["Close"] > 0.25) &
    (sorted_winners1["Rounded_Days"] >= 0) &
    (sorted_winners1["Days_x_Ratio"] >= 10) &
    (sorted_winners1["MY_RSI_RANK"] >= 0.75) &
    1 == 1
    ]


# In[8]:

# sorted_winners
