'''
Uses a starting amount already invested, plus monthly incremental investments increasing 
consistently every year and then estimates return values based on a random distribution from
the S&P 500 CAGR over the last 30 years, finally returned to a dollar amount representative of
today's purchasiny power by using the Consumer Price Index distribution from the last 33 years.
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random

# 1990 to 2023
# source: https://www.minneapolisfed.org/about-us/monetary-policy/inflation-calculator/consumer-price-index-1913-
us_cpi_annual_increases = [
  .054,
  .042,
  .030,
  .030,
  .026,
  .028,
  .029,
  .023,
  .016,
  .022,
  .034,
  .028,
  .016,
  .023,
  .027,
  .034,
  .032,
  .029,
  .038,
  -.004,
  .016,
  .032,
  .021,
  .015,
  .016,
  .001,
  .013,
  .021,
  .024,
  .018,
  .012,
  .047,
  .080,
  .041
]
us_cpi_mean = np.mean(us_cpi_annual_increases)
us_cpi_std = np.std(us_cpi_annual_increases)
us_cpi_rand_normal = np.random.normal(us_cpi_mean, us_cpi_std, 100000)
plt.hist(us_cpi_rand_normal, bins=50)

# Feb 1992 to Feb 2024 S&P 500 CAGR
# Divendend reinvested
# source: https://curvo.eu/backtest/en/market-index/sp-500?currency=usd
sp_cagr_rand_normal = np.random.normal(.1032, .1481, 100000)

plt.hist(sp_cagr_rand_normal, bins=50)




def total_investment_projection(starting_amt,
                                years,
                                starting_monthly_401k,
                                annual_increase_rate_401k,
                                starting_monthly_other,
                                annual_increase_rate_other,
                                retirement_withdrawl_rate):

    # 401k investments (Not including Supplemental Retirement Plan for Mgmt Employees)
    def monthly_401k_investments(starting_amt, annual_increase, years):
        month_investment_401k = starting_amt
        annual_401k_increase_pct = annual_increase
        investments_401k = []

        for i in range(years):
          investments_monthly_401k = list(np.repeat(month_investment_401k, 12))
          investments_401k = investments_401k + investments_monthly_401k
          month_investment_401k = \
            month_investment_401k+month_investment_401k*annual_401k_increase_pct

        return investments_401k


    monthly_401k = monthly_401k_investments(
      starting_amt=starting_monthly_401k,
      annual_increase=annual_increase_rate_401k,
      years=years
    )


    # Other Investments
    def monthly_other_investments(starting_amt, annual_increase_years, years):
        month_investment_amount = starting_amt
        annual_investment_increase_pct = annual_increase_years
        monthly_investments = []

        for i in range(years):
          year_investments = list(np.repeat(month_investment_amount, 12))

          monthly_investments = monthly_investments + year_investments

          month_investment_amount = \
            month_investment_amount+month_investment_amount*annual_investment_increase_pct

        return monthly_investments


    monthly_other = monthly_other_investments(
      starting_amt=starting_monthly_other,
      annual_increase_years=annual_increase_rate_other,
      years=years
    )

    
    # Combine 401k and other
    monthly_investments = [x+y for x,y in zip(monthly_401k, monthly_other)]
    

    
    # Randomly select a value from the cagr random norm list for each year of investment
    selected_sp_cargs = random.sample(list(sp_cagr_rand_normal), years)

    # Add 2 to each selected value
    monthly_sp_cagr = [(1 + value) ** (1 / 12) - 1 for value in selected_sp_cargs]

    # Repeat each modified value 12 times
    monthly_sp_cagr_vals = [val for val in monthly_sp_cagr for _ in range(12)]

    # Calculate the combination of total starting and monthly investments, that
    # include the monthly compounded interest from investment growth
    def calculate_future_value(starting_amount, monthly_investments):
        # Calculate future value with compound interest
        future_value = starting_amount
        for i,j in zip(range(len(monthly_sp_cagr_vals)), monthly_sp_cagr_vals):
            future_value *= (1 + j)  # Apply compound interest
            if i < len(monthly_investments):
                future_value += monthly_investments[i]  # Add monthly investments

        return future_value


    ending_amount = calculate_future_value(starting_amount=starting_amt,
                                           monthly_investments=monthly_investments)
    
    monthly_withdrawl = ending_amount*retirement_withdrawl_rate
    
    
    
    # Randomly select a value from the cpi rate random norm list for each year of investment
    cpi_rates = random.sample(list(us_cpi_rand_normal), years)
    
    # Calculate what the withdrawal rate would be in today's dollars
    inflation_adj_monthly_withdrawl = monthly_withdrawl
    for i in range(years):
        inflation_adj_monthly_withdrawl = inflation_adj_monthly_withdrawl / (1+cpi_rates[i])
    
    
    return ending_amount, monthly_withdrawl, inflation_adj_monthly_withdrawl
    
    
    




end_amts = []
monthly_livings = []
infl_adj_monthly_livings = []
for sim in range(10000):
    end_amt, monthly_living, infl_adj_monthly_living = total_investment_projection(
      starting_amt=,
      years=,
      starting_monthly_401k=,
      annual_increase_rate_401k=.03,
      starting_monthly_other=,
      annual_increase_rate_other=.03,
      retirement_withdrawl_rate = .04
    )
    
    end_amts.append(end_amt)
    monthly_livings.append(monthly_living)
    infl_adj_monthly_livings.append(infl_adj_monthly_living)
    

# quantiles of total ending amount
np.percentile(end_amts, 02.5)
np.percentile(end_amts, 50.0)
np.percentile(end_amts, 97.5)

# quantiles of monthly 4% withdrawal amounts
np.percentile(monthly_livings, 02.5)
np.percentile(monthly_livings, 50.0)
np.percentile(monthly_livings, 97.5)

# quantiles of monthly 4% withdrawal amounts at today's value
np.percentile(infl_adj_monthly_livings, 02.5)
np.percentile(infl_adj_monthly_livings, 50.0)
np.percentile(infl_adj_monthly_livings, 97.5)
