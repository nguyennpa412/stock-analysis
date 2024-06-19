import scrapy
import numpy as np
import pandas as pd


class SumPerChangeSpider(scrapy.Spider):
  name = 'sum_perchange'
  custom_settings = {
    # 'DOWNLOAD_DELAY': 0.1,
    # 'CONCURRENT_REQUESTS': 1
  }

  def __init__(self, *args, **kwargs):
    super(SumPerChangeSpider, self).__init__(*args, **kwargs)
    self.tvsi_hsx_code_url = './tvsi/tvsi_hsx_stock_code.csv'
    self.tvsi_hsx_code_cols = ['Mã chứng khoán', 'Ngành']
    self.tvsi_hsx_stock_data = {}
    self.tvsi_hsx_stock_list_data = []
    self.tvsi_hsx_stock_data_keys = ['code', 'name', 'sector', 'sum_perchange']
    self.stock_count = 0

    self.stock_name_url = lambda stock_code: f'https://finance.tvsi.com.vn/Enterprises/HistoricalQuotesCompany?symbol={stock_code}'
    self.data_url = lambda stock_code, page: f'https://finance.tvsi.com.vn/Enterprises/LichsugiaSymbolPart2?symbol={stock_code}&currentPage={page}&duration=d&startDate=01%2F01%2F2021&endDate=01%2F05%2F2021&_=1623711670747'
    
    self.sum_perchange_columns =  ['Mã chứng khoán', 'Tên chứng khoán', 'Ngành', '% Thay đổi']
    self.sum_perchange_outdir = './tvsi/sum_perchange'
    self.sum_perchange_name = 'tvsi_sum_perchange_01012021_01052021'
    self.top3_outdir = './tvsi/sum_perchange'
    self.top3_name = 'tvsi_top3_01012021_01052021'
    self.bot3_outdir = './tvsi/sum_perchange'
    self.bot3_name = 'tvsi_bot3_01012021_01052021'

  def start_requests(self):
    self.tvsi_hsx_code = pd.read_csv(self.tvsi_hsx_code_url).sort_values(by=self.tvsi_hsx_code_cols[0]).reset_index(drop=True)
    self.tvsi_hsx_code_len = len(self.tvsi_hsx_code)
    code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][self.stock_count]
    sector = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][self.stock_count]
    yield scrapy.Request(url=self.stock_name_url(code), callback=self.parse_stock_name, meta={self.tvsi_hsx_stock_data_keys[0]: code, self.tvsi_hsx_stock_data_keys[2]: sector, 'stock_count': self.stock_count})

    # yield scrapy.Request(url=
    #   self.stock_name_url(self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][112]), 
    #   callback=self.test, 
    #   meta={
    #     self.tvsi_hsx_stock_data_keys[0]: 
    #       self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][112], 
    #     self.tvsi_hsx_stock_data_keys[2]: 
    #       self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][112]
    #   }
    # )

  # def test(self, response):
  #   # stock_count = response.meta['stock_count']
  #   print(response.request.url)
  #   code = response.meta[self.tvsi_hsx_stock_data_keys[0]]
  #   sector = response.meta[self.tvsi_hsx_stock_data_keys[2]]
  #   # try:
  #   name = response.css('#analyze h3::text').extract()[0].split('-')[1].strip()
  #   self.tvsi_hsx_stock_data[code] = {self.tvsi_hsx_stock_data_keys[1]: name, self.tvsi_hsx_stock_data_keys[2]: sector, self.tvsi_hsx_stock_data_keys[3]: 0}
  #   print(name)
  #   # yield scrapy.Request(url=self.data_url(code, 1), callback=self.parse_stock_data_pages, meta={self.tvsi_hsx_stock_data_keys[0]: code, 'stock_count': stock_count})
  #   # except:
  #     # print('EXCEPT parse_stock_name')
  #     # self.stock_count += 1
  #     # if self.stock_count < self.tvsi_hsx_code_len:
  #     #   code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][self.stock_count]
  #     #   sector = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][self.stock_count]
  #     #   yield scrapy.Request(url=self.stock_name_url(code), callback=self.parse_stock_name, meta={self.tvsi_hsx_stock_data_keys[0]: code, self.tvsi_hsx_stock_data_keys[2]: sector, 'stock_count': self.stock_count})
  #     # else:
  #     #   print(self.tvsi_hsx_stock_data)

  def parse_stock_name(self, response):
    stock_count = response.meta['stock_count']
    code = response.meta[self.tvsi_hsx_stock_data_keys[0]]
    sector = response.meta[self.tvsi_hsx_stock_data_keys[2]]
    try:
      name = response.css('#analyze h3::text').extract()[0].split('-')[1].strip()
      self.tvsi_hsx_stock_data[code] = {self.tvsi_hsx_stock_data_keys[1]: name, self.tvsi_hsx_stock_data_keys[2]: sector, self.tvsi_hsx_stock_data_keys[3]: 0}
      yield scrapy.Request(url=self.data_url(code, 1), callback=self.parse_stock_data_pages, meta={self.tvsi_hsx_stock_data_keys[0]: code, 'stock_count': stock_count})
    except:
      print('EXCEPT parse_stock_name')
      self.stock_count += 1
      if self.stock_count < self.tvsi_hsx_code_len:
        code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][self.stock_count]
        sector = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][self.stock_count]
        yield scrapy.Request(url=self.stock_name_url(code), callback=self.parse_stock_name, meta={self.tvsi_hsx_stock_data_keys[0]: code, self.tvsi_hsx_stock_data_keys[2]: sector, 'stock_count': self.stock_count})
      else:
        self.postprocess()

  def parse_stock_data_pages(self, response):
    stock_count = response.meta['stock_count']
    code = response.meta[self.tvsi_hsx_stock_data_keys[0]]
    self.tvsi_hsx_stock_data[code][self.tvsi_hsx_stock_data_keys[3]] += self.cal_sum_perchange(response)
    self.print_percentage()
    try:
      pages = int(response.css('nav.fri li a::text').extract()[-1])
      if pages > 1:
        for page in range(pages - 1):
          yield scrapy.Request(url=self.data_url(code, page + 2), callback=self.parse_stock_data, meta={self.tvsi_hsx_stock_data_keys[0]: code, 'page': page + 2, 'pages': pages, 'stock_count': stock_count})
    except:
      print('EXCEPT parse_stock_data_pages')
      self.stock_count += 1
      if self.stock_count < self.tvsi_hsx_code_len:
        code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][self.stock_count]
        sector = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][self.stock_count]
        yield scrapy.Request(url=self.stock_name_url(code), callback=self.parse_stock_name, meta={self.tvsi_hsx_stock_data_keys[0]: code, self.tvsi_hsx_stock_data_keys[2]: sector, 'stock_count': self.stock_count})
      else:
        self.postprocess()

  
  def parse_stock_data(self, response):
    stock_count = response.meta['stock_count']
    code = response.meta[self.tvsi_hsx_stock_data_keys[0]]
    page = response.meta['page']
    pages = response.meta['pages']
    try:
      self.tvsi_hsx_stock_data[code][self.tvsi_hsx_stock_data_keys[3]] += self.cal_sum_perchange(response)
    except:
      print('EXCEPT parse_stock_data')
    if page == pages:
      self.stock_count += 1
      if self.stock_count < self.tvsi_hsx_code_len:
        code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][self.stock_count]
        sector = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[1]][self.stock_count]
        yield scrapy.Request(url=self.stock_name_url(code), callback=self.parse_stock_name, meta={self.tvsi_hsx_stock_data_keys[0]: code, self.tvsi_hsx_stock_data_keys[2]: sector, 'stock_count': self.stock_count})
      else:
        self.postprocess()

  def postprocess(self):
    for i in range(self.tvsi_hsx_code_len):
      code = self.tvsi_hsx_code[self.tvsi_hsx_code_cols[0]][i]
      if code in self.tvsi_hsx_stock_data:
        name = self.tvsi_hsx_stock_data[code][self.tvsi_hsx_stock_data_keys[1]]
        sector = self.tvsi_hsx_stock_data[code][self.tvsi_hsx_stock_data_keys[2]]
        sum_perchange = self.tvsi_hsx_stock_data[code][self.tvsi_hsx_stock_data_keys[3]]
        self.tvsi_hsx_stock_list_data.append([code, name, sector, sum_perchange])
    
    df = pd.DataFrame(np.array(self.tvsi_hsx_stock_list_data), columns=self.sum_perchange_columns)
    df[self.sum_perchange_columns[3]] = df[self.sum_perchange_columns[3]].astype(float)
    df = df.sort_values(by=self.sum_perchange_columns[3], ascending=False).reset_index(drop=True)

    top_3 = df[:3]
    bot_3 = df[-3:].sort_values(by=[self.sum_perchange_columns[3]]).reset_index(drop=True)

    # export to csv
    df.to_csv(f'{self.sum_perchange_outdir}/{self.sum_perchange_name}.csv', index=False, header=True)
    top_3.to_csv(f'{self.top3_outdir}/{self.top3_name}.csv', index=False, header=True)
    bot_3.to_csv(f'{self.bot3_outdir}/{self.bot3_name}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++TVSI-SUM_PERCHANGE++++++++++++++++++++++++++++')
    print(self.sum_perchange_name)
    print(df)
    print(self.top3_name)
    print(top_3)
    print(self.bot3_name)
    print(bot_3)
    print('================================================================================')
    
  def cal_sum_perchange(self, response):
    # get % change data
    perchange_data = response.css('tbody > tr > td > span::text').extract()[1::2]
    perchange_data = list(map(self.perchange_str_to_float, perchange_data))
    return sum(perchange_data)

  def print_percentage(self):
    if (self.stock_count % 10 == 0) | (self.stock_count == self.tvsi_hsx_code_len - 1):
      print(f'{self.stock_count:0>3d}/{self.tvsi_hsx_code_len - 1}: ' + ((100*self.stock_count)//(self.tvsi_hsx_code_len - 1))*'=' + '>' + (100 - (100*self.stock_count)//(self.tvsi_hsx_code_len - 1))*' ' + '|')
  
  @staticmethod
  def perchange_str_to_float(s):
    return float(s.split('%')[0])
