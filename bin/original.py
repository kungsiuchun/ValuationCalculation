import yfinance as yf
from datetime import datetime, date
import pandas as pd
import ipdb
import numpy as np
import matplotlib.pyplot as plt


class ValuationCalculation:
    def __init__(self,
                 code,
                 statement_dates,
                 financial_bases,
                 financial_pluses=None,
                 financial_base_name='Financial Base',
                 ratio_name='Ratio',
                 title=''):
        self.code = code
        self.statement_dates = statement_dates
        self.financial_pluses = financial_pluses
        self.financial_bases = financial_bases
        self.financial_base_name = financial_base_name
        self.ratio_name = ratio_name
        self.title = title

    def get_stock_prices(self):
        ticker = yf.Ticker(self.code)
        return ticker.history(period="max")

    def get_financial_statements(self, stock_prices):
        df = pd.DataFrame({
            'Date': self.statement_dates,
            self.financial_base_name: self.financial_bases,
            'Plus': [0 for x in self.statement_dates] if self.financial_pluses == None else self.financial_pluses,
        })
        df = df.set_index('Date')
        print(df)
        return df
        

    def interpolate_values(self, sheet):
        sheet = sheet.loc[sheet.index >= self.statement_dates[0]]
        sheet = sheet.interpolate(method='cubic')
        # sheet = sheet.loc[sheet.index > self.statement_dates[1]]
        sheet = sheet.loc[sheet.index <= datetime.today()]
        print(sheet)
        return sheet

    def add_financial_ratios(self, sheet):
        sheet[self.ratio_name] = (sheet['Close'] + sheet['Plus']) / \
            sheet[self.financial_base_name]
        
        return sheet

    def add_statistics(self, sheet):
        ratios = sheet[self.ratio_name]
        sheet['Mean'] = ratios.mean()
        sheet['Mean Price'] = sheet['Mean'] * \
            sheet[self.financial_base_name] - sheet['Plus']
        sheet['STD'] = ratios.std()
        print(sheet)
        for x in range(1, 3):
            sheet['STD+' + str(x)] = sheet['Mean'] + sheet['STD'] * x
            sheet['STD+' + str(x) + ' Price'] = sheet['STD+' + str(x)] * sheet[self.financial_base_name] - sheet['Plus']
            sheet['STD-' + str(x)] = sheet['Mean'] - sheet['STD'] * x
            sheet['STD-' + str(x) + ' Price'] = sheet['STD-' + str(x)] * sheet[self.financial_base_name] - sheet['Plus']
        print(sheet)
        return sheet

    def get_report(self):
        stock_prices = self.get_stock_prices()
        financial_statements = self.get_financial_statements(stock_prices)
        report = pd.merge(
            stock_prices,
            financial_statements,
            how='outer',
            left_index=True,
            right_index=True
        )
        report = self.interpolate_values(report)
        report = self.add_financial_ratios(report)
        report = self.add_statistics(report)
        ax = report[[self.ratio_name]].plot(color='#cccccc')
        report[['Close', 'Mean Price', 'STD+1 Price', 'STD+2 Price',
                'STD-1 Price', 'STD-2 Price']].plot(
                    secondary_y=True,
                    ax=ax, kind='line',
                    title=(self.code if self.title ==
                           '' else self.title + ' (' + self.code + ')'),
                    color=['black', 'green', 'orange', 'red', 'blue', 'purple'])
        plt.show()
        # report.to_csv(self.code + '.csv', sep=',', encoding='utf-8')


ValuationCalculation(
    code='BA',
    statement_dates=[datetime(x, 12, 30) for x in range(2006, 2020)],
    financial_bases=list(
        np.array([
            16.76,
            15.23,
            13.14,
            11.95,
            10.80,
            11.19,
            9.90,
            8.60,
            8.46,
            7.62,
            6.55,
            6.71,
            5.98,
            4.51
        ])
        # / 1_097_000 / (np.array([
        #     89.41,
        #     84.43,
        #     86.64,
        #     85.58,
        #     80.34,
        # ]) / 100)
    )[::-1],
    ratio_name='P/S',
).get_report()