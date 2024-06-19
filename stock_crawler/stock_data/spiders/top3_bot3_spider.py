import scrapy
import pandas as pd


class Top3Bot3Spider(scrapy.Spider):
  name = 'top3_bot3'

  def __init__(self, *args, **kwargs):
    super(Top3Bot3Spider, self).__init__(*args, **kwargs)
    self.top3 = ['DCL', 'RIC', 'FUCTVGF1']
    self.bot3 = ['PXT', 'TGG', 'ABS']
    self.raw_dir = './vietstock/influence/raw_top3_bot3_01052019_01052021'

    self.data_url = lambda stock_code : f'https://finance.vietstock.vn/data/ExportTradingResult?Code={stock_code}&OrderBy=&OrderDirection=desc&PageIndex=3&PageSize=30&FromDate=2019-05-01&ToDate=2021-05-01&ExportType=excel&Cols=GTC%2CTKLGD%2CTGTGD%2CVHTT%2CMC%2CTGG%2CDC%2CTGPTG%2CCN%2CTN%2CKLGDKL%2CGTGDKL%2CKLGDTT%2CGTGDTT&ExchangeID=1'

    self.df_columns = ['Ngày', 'Tham chiếu', 'Tổng khối lượng GD', 'Tổng giá trị GD', 'Vốn hóa Thị trường', 
                      'Mở cửa', 'Đóng cửa', 'Cao nhất', 'Thấp nhất', 'Thay đổi', '% Thay đổi', 
                      'Khối lượng GD Khớp lệnh', 'Giá trị GD Khớp lệnh',
                      'Khối lượng GD Thỏa thuận', 'Giá trị GD Thoả thuận']                    

    self.top3_outdir = './vietstock/influence/top3'
    self.bot3_outdir = './vietstock/influence/bot3'

  def start_requests(self):
    yield scrapy.Request(url='https://finance.vietstock.vn/', callback=self.parse)

  def parse(self, response):
    for i in range(3):
      top = pd.read_csv(f'{self.raw_dir}/{self.top3[i]}.csv', names=self.df_columns)
      top[self.df_columns[3]] = top[self.df_columns[3]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      top[self.df_columns[12]] = top[self.df_columns[12]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      top[self.df_columns[14]] = top[self.df_columns[14]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      top.to_csv(f'{self.top3_outdir}/{self.top3[i]}.csv', index=False, header=True)

      bot = pd.read_csv(f'{self.raw_dir}/{self.bot3[i]}.csv', names=self.df_columns)
      bot[self.df_columns[3]] = bot[self.df_columns[3]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      bot[self.df_columns[12]] = bot[self.df_columns[12]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      bot[self.df_columns[14]] = bot[self.df_columns[14]].apply(lambda x: float(''.join(x.split(',')))*1e6 if isinstance(x, str) else x*1e6)
      bot.to_csv(f'{self.bot3_outdir}/{self.bot3[i]}.csv', index=False, header=True)