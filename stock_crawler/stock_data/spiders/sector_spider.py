import scrapy
import numpy as np
import pandas as pd
from urllib import parse

class SectorSpider(scrapy.Spider):
  name = 'sector'

  def __init__(self, *args, **kwargs):
    super(SectorSpider, self).__init__(*args, **kwargs)
    self.sectors = ['cntt', 'bds', 'dvltaugt']
    self.urls = {
      self.sectors[0]: 'https://api.vietstock.vn/finance/sectorinfo?sectorID=6&languageID=1',
      self.sectors[1]: 'https://api.vietstock.vn/finance/sectorinfo?sectorID=3&languageID=1',
      self.sectors[2]: 'https://api.vietstock.vn/finance/sectorinfo?sectorID=25&languageID=1'
    }
    self.columns =  ['Tên chứng khoán', 'Mã chứng khoán', 'Sàn giao dịch', 'Ngành cấp 3']
    self.sector_by_id = {
      '6': self.sectors[0],
      '3': self.sectors[1],
      '25': self.sectors[2]
    }
    self.outdir = './vietstock/sector'

  def start_requests(self):
    for sector in self.sectors:
      yield scrapy.Request(url=self.urls[sector], callback=self.parse)

  def parse(self, response):
    # get sector id
    sector_id = parse.parse_qs(parse.urlparse(response.request.url).query)['sectorID'][0]

    # get stock name, stock code, stock exchange, level 3 industry
    stock_name = response.xpath('//stockname/text()').getall()
    stock_code = response.xpath('//_sc_/text()').getall()
    catID = response.text.split('<catID>')[1:]
    stock_exchange = []
    for line in catID:
      if line[0] == '1':
        stock_exchange.append('HOSE')
      elif line[0] == '2':
        stock_exchange.append('HNX')
      else:
        stock_exchange.append('')
    level3_industry = response.xpath('//_sin_/text()').getall()
    
    # dataframe
    sector_df = pd.DataFrame(np.c_[stock_name, stock_code, stock_exchange, level3_industry], columns=self.columns).sort_values(by=[self.columns[2]])

    # export to csv
    sector_df.to_csv(f'{self.outdir}/{self.sector_by_id[sector_id]}.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++VIETSTOCK-{self.sector_by_id[sector_id]}++++++++++++++++++++++++++++')
    print(self.sector_by_id[sector_id])
    print(sector_df)
    print('================================================================================')