import scrapy
import json
import numpy as np
import pandas as pd


class InfluenceSpider(scrapy.Spider):
  name = 'influence'

  def __init__(self, *args, **kwargs):
    super(InfluenceSpider, self).__init__(*args, **kwargs)
    self.raw_data = './vietstock/influence/raw_data_01012021_01052021.txt'
    self.influence_columns =  ['Mã chứng khoán', 'Giá', 'Thay đổi', '% Thay đổi', 'Khối lượng cổ phiếu lưu hành', 'Vốn hóa', 'Tỷ trọng', '% Ảnh hưởng', 'Điểm ảnh hưởng']
    self.influence_keys = ['StockCode', 'ClosePrice', 'Change', 'PerChange', 'KLCPLH', 'MarketCap', 'Weight', 'InfluencePercent', 'InfluenceIndex']
    self.influence_keys_len = len(self.influence_keys)
    self.outdir = './vietstock/influence'

  def start_requests(self):
    yield scrapy.Request(url='https://finance.vietstock.vn/', callback=self.parse_influence)

  def parse_influence(self, response):
    # read data from raw file
    influence_raw_data = json.loads(open(self.raw_data, 'r').read())
    influence_data = []
    for row in influence_raw_data:
      influence_data.append([row[self.influence_keys[i]] for i in range(self.influence_keys_len)])

    # dataframe
    influence_df = pd.DataFrame(np.array(influence_data), columns=self.influence_columns)
    influence_df = pd.concat([influence_df[influence_df[self.influence_columns[3]] >= '0'].sort_values(by=[self.influence_columns[3]], ascending=False),
                              influence_df[influence_df[self.influence_columns[3]] < '0'].sort_values(by=[self.influence_columns[3]])]).reset_index(drop=True)
    top_3 = influence_df[:3]
    bot_3 = influence_df[-3:]

    # export to csv
    influence_df.to_csv(f'{self.outdir}/influence.csv', index=False, header=True)
    top_3.to_csv(f'{self.outdir}/top3.csv', index=False, header=True)
    bot_3.to_csv(f'{self.outdir}/bot3.csv', index=False, header=True)

    print(f'++++++++++++++++++++++++++++VIETSTOCK-influence++++++++++++++++++++++++++++')
    print('influence')
    print(influence_df)
    print('top_3')
    print(top_3)
    print('bot_3')
    print(bot_3)
    print('================================================================================')