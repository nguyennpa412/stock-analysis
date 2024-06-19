import scrapy
import numpy as np
import pandas as pd


class AsyncTop3Bot3SumPerChangeSpider(scrapy.Spider):
  name = 'async_top3_bot3_sum_perchange'

  def __init__(self, *args, **kwargs):
    super(AsyncTop3Bot3SumPerChangeSpider, self).__init__(*args, **kwargs)
    self.tvsi_top3 = './tvsi/sum_perchange/tvsi_top3_01012021_01052021.csv'
    self.tvsi_bot3 = './tvsi/sum_perchange/tvsi_bot3_01012021_01052021.csv'
    self.keys = ['code', 'name', 'sector', 'sum_perchange']

    self.data_url = lambda stock_code, page: f'https://finance.tvsi.com.vn/Enterprises/LichsugiaSymbolPart2?symbol={stock_code}&currentPage={page}&duration=d&startDate=01%2F05%2F2019&endDate=13%2F06%2F2021&_=1623728202137'
    self.pages = 18

    self.sum_perchange_columns =  ['Mã chứng khoán', 'Tên chứng khoán', 'Ngành', '% Thay đổi']
    self.tvsi_columns =  ['Ngày', 'Mở cửa', 'Đóng cửa', 'Thay đổi', '% Thay đổi', 'Khối lượng GD Khớp lệnh',
                          'Giá trị GD Khớp lệnh', 'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận',
                          'Tổng khối lượng GD', 'Tổng giá trị GD']  

    self.top3_data = {}
    self.bot3_data = {}

    self.top3_modeling_outdir = './modeling_data/top3'
    self.top3_modeling_name = lambda stock_code: f'tvsi_{stock_code}_01052019_13062021'
    self.bot3_modeling_outdir = './modeling_data/bot3'
    self.bot3_modeling_name = lambda stock_code: f'tvsi_{stock_code}_01052019_13062021'

  def start_requests(self):
    tvsi_top3_list = pd.read_csv(self.tvsi_top3)
    tvsi_bot3_list = pd.read_csv(self.tvsi_bot3)
    tvsi_top3_code = list(tvsi_top3_list[self.sum_perchange_columns[0]])
    tvsi_bot3_code = list(tvsi_bot3_list[self.sum_perchange_columns[0]])
    page = 1

    for i in range(3):
      top_code = tvsi_top3_code[i]
      self.top3_data[top_code] = {}
      bot_code = tvsi_bot3_code[i]
      self.bot3_data[bot_code] = {}

      yield scrapy.Request(url=self.data_url(top_code, page), 
                          callback=self.parse_top_page,
                          meta={'code': top_code, 'page': page})
      yield scrapy.Request(url=self.data_url(bot_code, page), 
                          callback=self.parse_bot_page,
                          meta={'code': bot_code, 'page': page})

  def parse_top_page(self, response):
    code = response.meta['code']
    page = response.meta['page']
    if page not in self.top3_data[code]:
      self.top3_data[code][page] = []
    try:
      self.top3_data[code][page] = self.add_data(response)
    except:
      print('EXCEPT parse_top_page')
    if page == self.pages:
      data = []
      for i in range(self.pages):
        data += self.top3_data[code][i + 1]
      self.export(data, self.top3_modeling_outdir, self.top3_modeling_name(code))
    else:
      yield scrapy.Request(url=self.data_url(code, page + 1), 
                          callback=self.parse_top_page,
                          meta={'code': code, 'page': page + 1})

  def parse_bot_page(self, response):
    code = response.meta['code']
    page = response.meta['page']
    if page not in self.bot3_data[code]:
      self.bot3_data[code][page] = []
    try:
      self.bot3_data[code][page] = self.add_data(response)
    except:
      print('EXCEPT parse_bot_page')
    if page == self.pages:
      data = []
      for i in range(self.pages):
        data += self.bot3_data[code][i + 1]
      self.export(data, self.bot3_modeling_outdir, self.bot3_modeling_name(code))
    else:
      yield scrapy.Request(url=self.data_url(code, page + 1), 
                          callback=self.parse_bot_page,
                          meta={'code': code, 'page': page + 1})

  def add_data(self, response):
    # get table data
    res = []
    td_data = response.css('tbody > tr > td::text').extract()
    td_data_len = len(td_data)
    span_data = response.css('tbody > tr > td > span::text').extract()

    j = 0
    for i in np.arange(0, td_data_len - 1, 9):
      res.append([td_data[i], self.str_to_float(td_data[i+1]), self.str_to_float(td_data[i+2])]
      + list(map(self.perchange_str_to_float, span_data[j:j+2]))
      + [self.str_to_float(td_data[i+3]), self.str_to_float(td_data[i+6])*1e9, self.str_to_float(td_data[i+4]),
        self.str_to_float(td_data[i+7])*1e9, self.str_to_float(td_data[i+5]), self.str_to_float(td_data[i+8])*1e9])
      j += 2

    return res

  def export(self, data, outdir, name):
    # dataframe
    df = pd.DataFrame(np.array(data), columns=self.tvsi_columns)

    # export to csv
    df.to_csv(f'{outdir}/{name}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++TVSI-{name}++++++++++++++++++++++++++++')
    print(name)
    print(df)
    print('================================================================================')

  @staticmethod
  def str_to_float(s):
    return float(''.join(s.split(',')))

  @staticmethod
  def perchange_str_to_float(s):
    return float(s.split('%')[0])