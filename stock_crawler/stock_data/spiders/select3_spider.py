import scrapy
import numpy as np
import pandas as pd

class Select3Spider(scrapy.Spider):
  name = 'select3'

  def __init__(self, *args, **kwargs):
    super(Select3Spider, self).__init__(*args, **kwargs)
    self.selected_codes = ['VIC', 'FPT', 'HVN']

    self.data_url = lambda stock_code, page: f'https://finance.tvsi.com.vn/Enterprises/LichsugiaSymbolPart2?symbol={stock_code}&currentPage={page}&duration=d&startDate=01%2F05%2F2019&endDate=13%2F06%2F2021&_=1623728202137'
    self.pages = 18

    self.tvsi_columns =  ['Ngày', 'Mở cửa', 'Đóng cửa', 'Thay đổi', '% Thay đổi', 'Khối lượng GD Khớp lệnh',
                          'Giá trị GD Khớp lệnh', 'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận',
                          'Tổng khối lượng GD', 'Tổng giá trị GD']

    self.data = {}

    self.select3_modeling_outdir = './modeling_data/select3'
    self.select3_modeling_name = lambda stock_code: f'tvsi_{stock_code}_01052019_13062021'

  def start_requests(self):
    page = 1

    for i in range(3):
      code = self.selected_codes[i]
      self.data[code] = {}

      yield scrapy.Request(url=self.data_url(code, page), 
                          callback=self.parse_page,
                          meta={'code': code, 'page': page})

  def parse_page(self, response):
    code = response.meta['code']
    page = response.meta['page']
    if page not in self.data[code]:
      self.data[code][page] = []
    try:
      self.data[code][page] = self.add_data(response)
    except:
      print('EXCEPT parse_page')
    if page == self.pages:
      data_ = []
      for i in range(self.pages):
        data_ += self.data[code][i + 1]
      self.export(data_, self.select3_modeling_outdir, self.select3_modeling_name(code))
    else:
      yield scrapy.Request(url=self.data_url(code, page + 1), 
                          callback=self.parse_page,
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