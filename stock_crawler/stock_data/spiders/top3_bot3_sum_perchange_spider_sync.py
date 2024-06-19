import scrapy
import numpy as np
import pandas as pd


class Top3Bot3SumPerChangeSpider(scrapy.Spider):
  name = 'top3_bot3_sum_perchange'

  def __init__(self, *args, **kwargs):
    super(Top3Bot3SumPerChangeSpider, self).__init__(*args, **kwargs)
    self.tvsi_top3 = './tvsi/sum_perchange/tvsi_top3_01012021_01052021.csv'
    self.tvsi_bot3 = './tvsi/sum_perchange/tvsi_bot3_01012021_01052021.csv'
    self.keys = ['code', 'name', 'sector', 'sum_perchange']

    self.data_url = lambda stock_code, page: f'https://finance.tvsi.com.vn/Enterprises/LichsugiaSymbolPart2?symbol={stock_code}&currentPage={page}&duration=d&startDate=01%2F05%2F2019&endDate=13%2F06%2F2021&_=1623728202137'
    
    self.sum_perchange_columns =  ['Mã chứng khoán', 'Tên chứng khoán', 'Ngành', '% Thay đổi']
    self.tvsi_columns =  ['Ngày', 'Mở cửa', 'Đóng cửa', 'Thay đổi', '% Thay đổi', 'Khối lượng GD Khớp lệnh',
                          'Giá trị GD Khớp lệnh', 'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận',
                          'Tổng khối lượng GD', 'Tổng giá trị GD']  

    self.top3_modeling_outdir = './modeling_data/top3'
    self.top3_modeling_name = lambda stock_code: f'tvsi_{stock_code}_01052019_13062021'
    self.bot3_modeling_outdir = './modeling_data/bot3'
    self.bot3_modeling_name = lambda stock_code: f'tvsi_{stock_code}_01052019_13062021'

  def start_requests(self):
    tvsi_top3_list = pd.read_csv(self.tvsi_top3)
    tvsi_bot3_list = pd.read_csv(self.tvsi_bot3)
    tvsi_top3_code = list(tvsi_top3_list[self.sum_perchange_columns[0]])
    tvsi_bot3_code = list(tvsi_bot3_list[self.sum_perchange_columns[0]])
    self.top3_data = {}
    self.bot3_data = {}
    top_cls = []
    bot_cls = []
    top_outdir = []
    bot_outdir = []
    top_name = []
    bot_name = []

    for i in range(3):
      self.top3_data[tvsi_top3_code[i]] = []
      self.bot3_data[tvsi_bot3_code[i]] = []
      top_cls.append('top')
      bot_cls.append('bot')
      top_outdir.append(self.top3_modeling_outdir)
      bot_outdir.append(self.bot3_modeling_outdir)
      top_name.append(self.top3_modeling_name)
      bot_name.append(self.bot3_modeling_name)

    self.data = {'top': self.top3_data, 'bot': self.bot3_data}
    self.codes = tvsi_top3_code + tvsi_bot3_code
    self.cls = top_cls + bot_cls
    self.outdir = top_outdir + bot_outdir
    self.name = top_name + bot_name
    self.idx = 0

    start_code = self.codes[self.idx]

    yield scrapy.Request(url=self.data_url(start_code, 1), 
                         callback=self.parse_pages)

  def parse_pages(self, response):
    self.add_data(response, self.data[self.cls[self.idx]][self.codes[self.idx]])
    try:
      pages = int(response.css('nav.fri li a::text').extract()[-1])
      if pages > 1:
        for page in range(pages - 1):
          yield scrapy.Request(url=self.data_url(self.codes[self.idx], page + 2), 
                               callback=self.parse_page, 
                               meta={'page': page + 2, 
                                    'pages': pages})
    except:
      print('EXCEPT parse_pages')
      print('==============================')
      print('before', self.idx)
      self.idx += 1
      if self.idx < 6:
        print('<6', self.idx)
        yield scrapy.Request(url=self.data_url(self.codes[self.idx], 1), 
                            callback=self.parse_pages)
      else:
        print('>=6', self.idx)
        self.postprocess()
      print('==============================')
      

  def parse_page(self, response):
    page = response.meta['page']
    pages = response.meta['pages']
    try:
      self.add_data(response, self.data[self.cls[self.idx]][self.codes[self.idx]])
    except:
      print('EXCEPT parse_page')
    if page == pages:
      print('==============================')
      print('before', self.idx)
      self.idx += 1
      if self.idx < 6:
        print('<6', self.idx)
        yield scrapy.Request(url=self.data_url(self.codes[self.idx], 1), 
                            callback=self.parse_pages)
      else:
        print('>=6', self.idx)
        self.postprocess()
      print('==============================')

  def add_data(self, response, arr):
    # get table data
    td_data = response.css('tbody > tr > td::text').extract()
    td_data_len = len(td_data)
    span_data = response.css('tbody > tr > td > span::text').extract()

    j = 0
    for i in np.arange(0, td_data_len - 1, 9):
      arr.append([td_data[i], self.str_to_float(td_data[i+1]), self.str_to_float(td_data[i+2])]
      + list(map(self.perchange_str_to_float, span_data[j:j+2]))
      + [self.str_to_float(td_data[i+3]), self.str_to_float(td_data[i+6])*1e9, self.str_to_float(td_data[i+4]),
        self.str_to_float(td_data[i+7])*1e9, self.str_to_float(td_data[i+5]), self.str_to_float(td_data[i+8])*1e9])
      j += 2

  def postprocess(self):
    for i in range(6):
      # dataframe
      df = pd.DataFrame(np.array(self.data[self.cls[i]][self.codes[i]]), columns=self.tvsi_columns)

      # export to csv
      df.to_csv(f'{self.outdir[i]}/{self.name[i](self.codes[i])}.csv', index=False, header=True)

      print(f'++++++++++++++++++++++++++++TVSI-{self.name[i](self.codes[i])}++++++++++++++++++++++++++++')
      print(self.name[i](self.codes[i]))
      print(df)
      print('================================================================================')


  @staticmethod
  def str_to_float(s):
    return float(''.join(s.split(',')))

  @staticmethod
  def perchange_str_to_float(s):
    return float(s.split('%')[0])