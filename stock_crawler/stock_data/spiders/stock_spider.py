import scrapy
import json
import numpy as np
import pandas as pd
from datetime import datetime
from urllib import parse


class StockSpider(scrapy.Spider):
  name = 'stock'
  custom_settings = {
    # 'LOG_LEVEL': 'INFO'
  }

  def __init__(self, *args, **kwargs):
    super(StockSpider, self).__init__(*args, **kwargs)
    self.stock_codes = ['vnindex', 'hnxindex', 'upcom']

    # vietstock
    self.vietstock_urls = {
      self.stock_codes[0]: [f'https://finance.vietstock.vn/data/KQGDThongKeGiaStockPaging?page={i + 1}&pageSize=20&catID=1&stockID=-19&fromDate=2019-05-01&toDate=2021-05-01' for i in range(12)],
      self.stock_codes[1]: [f'https://finance.vietstock.vn/data/KQGDThongKeGiaStockPaging?page={i + 1}&pageSize=20&catID=2&stockID=-18&fromDate=2019-05-01&toDate=2021-05-01' for i in range(12)],
      self.stock_codes[2]: [f'https://finance.vietstock.vn/data/KQGDThongKeGiaStockPaging?page={i + 1}&pageSize=20&catID=3&stockID=-17&fromDate=2019-05-01&toDate=2021-05-01' for i in range(12)]
    }
    # self.vietstock_columns =  ['Trading Date', 'Basic Price', 'Open Price', 'Close Price', 'Highest Price', 'Lowest Price', 'Change', 'Percentage Change',
    #                 'Matching Orders Total Volume', 'Matching Orders Total Value', 'Put Through Total Volume', 'Put Through Total Value',
    #                 'Total Volume', 'Total Value', 'Market Capitalization']
    self.vietstock_columns = ['Ngày', 'Tham chiếu', 'Mở cửa', 'Đóng cửa', 'Cao nhất', 'Thấp nhất',
                              'Thay đổi', '% Thay đổi', 'Khối lượng GD Khớp lệnh', 'Giá trị GD Khớp lệnh',
                              'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận', 'Tổng khối lượng GD',
                              'Tổng giá trị GD', 'Vốn hóa Thị trường']                    
    self.vietstock_stock_data = {
      self.stock_codes[0]: {},
      self.stock_codes[1]: {},
      self.stock_codes[2]: {}
    }
    self.vietstock_page_count = {
      self.stock_codes[0]: 0,
      self.stock_codes[1]: 0,
      self.stock_codes[2]: 0
    }
    self.vietstock_outdir = './vietstock/stock'

    # tvsi
    self.tvsi_urls = {
      self.stock_codes[0]: 'https://finance.tvsi.com.vn/data/ListLichsugia?currentPage=1&pageSize=528&duration=d&symbol=HOSTC&startDate=01%2F05%2F2019&endDate=01%2F05%2F2021&_=1623224004038',
      self.stock_codes[1]: 'https://finance.tvsi.com.vn/data/ListLichsugia?currentPage=1&pageSize=528&duration=d&symbol=HASTC&startDate=01%2F05%2F2019&endDate=01%2F05%2F2021&_=1623224004039',
      self.stock_codes[2]: 'https://finance.tvsi.com.vn/data/ListLichsugia?currentPage=1&pageSize=528&duration=d&symbol=UPCOM&startDate=01%2F05%2F2019&endDate=01%2F05%2F2021&_=1623224004040'
    }
    self.tvsi_columns =  ['Ngày', 'Mở cửa', 'Đóng cửa', 'Thay đổi', '% Thay đổi', 'Khối lượng GD Khớp lệnh',
                          'Giá trị GD Khớp lệnh', 'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận',
                          'Tổng khối lượng GD', 'Tổng giá trị GD']  
    self.tvsi_stock_data = {
      self.stock_codes[0]: [],
      self.stock_codes[1]: [],
      self.stock_codes[2]: []
    }
    self.tvsi_outdir = './tvsi/stock'
    
    # tvsi vnindex from 01/05/2019 to 13/06/2021
    self.tvsi_vnindex_for_modeling_url = 'https://finance.tvsi.com.vn/data/ListLichsugia?currentPage=1&pageSize=1000&duration=d&symbol=HOSTC&startDate=01%2F05%2F2019&endDate=13%2F06%2F2021&_=1623601750403'
    self.tvsi_vnindex_for_modeling_data = []
    self.tvsi_vnindex_for_modeling_csv = 'tvsi_vnindex_01052019_13062021'
    self.tvsi_vnindex_for_modeling_outdir = './modeling_data/vnindex'

    # vietstock vnindex from _ to 13/06/2021
    self.vietstock_vnindex_for_modeling_url = lambda page: f'https://finance.vietstock.vn/data/KQGDThongKeGiaStockPaging?page={page}&pageSize=20&catID=1&stockID=-19&fromDate=2019-05-01&toDate=2021-06-13'
    self.vietstock_vnindex_for_modeling_data = []
    self.vietstock_vnindex_for_modeling_pages = 13
    self.vietstock_vnindex_for_modeling_csv = 'vietstock_vnindex_08062020_13062021'
    self.vietstock_vnindex_for_modeling_outdir = './modeling_data/vnindex'

  def start_requests(self):
    for stock_code in self.stock_codes:
    #   for url in self.vietstock_urls[stock_code]:
    #     yield scrapy.Request(url=url, callback=self.parse_vietstock)
      yield scrapy.Request(url=self.tvsi_urls[stock_code], callback=self.parse_tvsi)

    # crawl data for modeling
    # yield scrapy.Request(url=self.vietstock_vnindex_for_modeling_url(1), 
    #                     callback=self.parse_vietstock_vnindex_for_modeling,
    #                     meta={'page': 1})
    # yield scrapy.Request(url=self.tvsi_vnindex_for_modeling_url, callback=self.parse_tvsi_vnindex_for_modeling)

  def parse_vietstock(self, response):
    # get response data
    data = json.loads(response.text)[1]

    # check stock_code
    stock_code = ''
    if data[0]['StockCode'] == 'VN-Index':
      stock_code = self.stock_codes[0]
    elif data[0]['StockCode'] == 'HNX-Index':
      stock_code = self.stock_codes[1]
    elif data[0]['StockCode'] == 'UPCOM-Index':
      stock_code = self.stock_codes[2]

    # transform text data and append to data array
    stock_data = []
    for row in data:
      trading_date = datetime.fromtimestamp(int(row['TradingDate'][6:-2])//1000)
      day = trading_date.day if int(trading_date.day) >= 10 else f'0{trading_date.day}'
      month = trading_date.month if int(trading_date.month) >= 10 else f'0{trading_date.month}'
      year = trading_date.year
      trading_date = f'{day}/{month}/{year}'
      stock_data.append([trading_date, row['BasicPrice'], row['OpenPrice'], row['ClosePrice'], row['HighestPrice'],
                        row['LowestPrice'], row['Change'], row['PerChange'], row['M_TotalVol'],
                        1e6*float(row['M_TotalVal']), row['PT_TotalVol'], 1e6*float(row['PT_TotalVal']),
                        row['TotalVol'], 1e6*float(row['TotalVal']), 1e6*float(row['MarketCap'])])
    
    # transform to pandas DataFrame
    df = pd.DataFrame(np.array(stock_data), columns=self.vietstock_columns)

    # get page
    page = parse.parse_qs(parse.urlparse(response.request.url).query)['page'][0]

    # save data to page dict
    self.vietstock_stock_data[stock_code][page] = df

    # count page
    self.vietstock_page_count[stock_code] += 1

    # check if all pages gotten
    if self.vietstock_page_count[stock_code] == 12:
      stock_df = pd.DataFrame(columns=self.vietstock_columns)
      for i in range(12):
        # concat pages in order
        stock_df = pd.concat([stock_df, self.vietstock_stock_data[stock_code][str(i + 1)]])
      
      # # re-order DataFrame
      # stock_df = stock_df[::-1].reset_index(drop=True)

      # export to csv
      stock_df.to_csv(f'{self.vietstock_outdir}/{stock_code}.csv', index=False, header=True)

      print(f'++++++++++++++++++++++++++++VIETSTOCK-{stock_code}++++++++++++++++++++++++++++')
      print(stock_code)
      print(stock_df)
      print('================================================================================')

  def parse_tvsi(self, response):
    # get stock code
    symbol = parse.parse_qs(parse.urlparse(response.request.url).query)['symbol'][0]
    stock_code = self.stock_codes[0]
    if symbol == 'HASTC':
      stock_code = self.stock_codes[1]
    elif symbol == 'UPCOM':
      stock_code = self.stock_codes[2]

    # get table data
    td_data = response.css('tbody > tr > td::text').extract()
    td_data_len = len(td_data)
    span_data = response.css('tbody > tr > td > span::text').extract()

    j = 0
    for i in np.arange(0, td_data_len - 1, 9):
      self.tvsi_stock_data[stock_code].append([td_data[i], self.str_to_float(td_data[i+1]), self.str_to_float(td_data[i+2])]
      + [float(span_data[j])] + [float(str(span_data[j + 1]).split('%')[0])]
      + [self.str_to_float(td_data[i+3]), self.str_to_float(td_data[i+6])*1e9, self.str_to_float(td_data[i+4]),
        self.str_to_float(td_data[i+7])*1e9, self.str_to_float(td_data[i+5]), self.str_to_float(td_data[i+8])*1e9])
      j += 2
    
    # dataframe
    stock_df = pd.DataFrame(self.tvsi_stock_data[stock_code], columns=self.tvsi_columns)

    # export to csv
    stock_df.to_csv(f'{self.tvsi_outdir}/{stock_code}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++TVSI-{stock_code}++++++++++++++++++++++++++++')
    print(stock_code)
    print(stock_df)
    print('================================================================================')

  def parse_tvsi_vnindex_for_modeling(self, response):
    # get table data
    td_data = response.css('tbody > tr > td::text').extract()
    td_data_len = len(td_data)
    span_data = response.css('tbody > tr > td > span::text').extract()

    j = 0
    for i in np.arange(0, td_data_len - 1, 9):
      self.tvsi_vnindex_for_modeling_data.append([td_data[i], self.str_to_float(td_data[i+1]), self.str_to_float(td_data[i+2])]
      + [float(span_data[j])] + [float(str(span_data[j + 1]).split('%')[0])]
      + [self.str_to_float(td_data[i+3]), self.str_to_float(td_data[i+6])*1e9, self.str_to_float(td_data[i+4]),
        self.str_to_float(td_data[i+7])*1e9, self.str_to_float(td_data[i+5]), self.str_to_float(td_data[i+8])*1e9])
      j += 2
    
    # dataframe
    stock_df = pd.DataFrame(self.tvsi_vnindex_for_modeling_data, columns=self.tvsi_columns)

    # export to csv
    stock_df.to_csv(f'{self.tvsi_vnindex_for_modeling_outdir}/{self.tvsi_vnindex_for_modeling_csv}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++TVSI-{self.tvsi_vnindex_for_modeling_csv}++++++++++++++++++++++++++++')
    print(self.tvsi_vnindex_for_modeling_csv)
    print(stock_df)
    print('================================================================================')

  def parse_vietstock_vnindex_for_modeling(self, response):
    page = response.meta['page']

    try:
      # get response data
      data = json.loads(response.text)[1]
      for row in data:
        trading_date = datetime.fromtimestamp(int(row['TradingDate'][6:-2])//1000)
        day = trading_date.day if int(trading_date.day) >= 10 else f'0{trading_date.day}'
        month = trading_date.month if int(trading_date.month) >= 10 else f'0{trading_date.month}'
        year = trading_date.year
        trading_date = f'{day}/{month}/{year}'
        self.vietstock_vnindex_for_modeling_data.append(
          [trading_date, row['BasicPrice'], row['OpenPrice'],
          row['ClosePrice'], row['HighestPrice'],
          row['LowestPrice'], row['Change'],
          row['PerChange'], row['M_TotalVol'],
          1e6*float(row['M_TotalVal']), row['PT_TotalVol'],
          1e6*float(row['PT_TotalVal']), row['TotalVol'],
          1e6*float(row['TotalVal']), 1e6*float(row['MarketCap'])])
    except:
      print('EXCEPT parse_vietstock_vnindex_for_modeling')

    if page == self.vietstock_vnindex_for_modeling_pages:
      self.export(self.vietstock_vnindex_for_modeling_data, self.vietstock_columns, self.vietstock_vnindex_for_modeling_outdir, self.vietstock_vnindex_for_modeling_csv)
    else:
      yield scrapy.Request(url=self.vietstock_vnindex_for_modeling_url(page + 1), 
                        callback=self.parse_vietstock_vnindex_for_modeling,
                        meta={'page': page + 1})

  def export(self, data, col, outdir, name):
    # dataframe
    df = pd.DataFrame(np.array(data), columns=col)

    # export to csv
    df.to_csv(f'{outdir}/{name}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++{name}++++++++++++++++++++++++++++')
    print(name)
    print(df)
    print('================================================================================')  

  @staticmethod
  def str_to_float(s):
    return float(''.join(s.split(',')))