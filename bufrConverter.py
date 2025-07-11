from pybufrkit.decoder import Decoder
from pybufrkit.renderer import FlatJsonRenderer
import os
from glob import glob
from pprint import pprint



class BufrConverter:
    def __init__(self):
      self.TOPDIR = os.path.dirname(os.path.abspath(__file__))
      self.decoder = Decoder(tables_root_dir=f"{self.TOPDIR}/tables")
      self.renderer = FlatJsonRenderer()
      self.ori_data = []
      self.__clear_splited_data()
   
    def __clear_splited_data(self):
      self.indicative_sections = []
      self.identificative_sections = []
      self.data_descriptive_sections = []
      self.data_sections = []
      self.end_sections = []

    def convert(self):
      DATA_PATH = os.path.join(os.getcwd(), "data")
      file_names = glob(f"{DATA_PATH}/*/IUPC44*.send")

      ans_json = []
      for file_name in file_names:
        with open(file_name, 'rb') as buf:
          bufr_message = self.decoder.process(buf.read())
        json = self.renderer.render(bufr_message)

        ans_json.append(json)

     
      self.ori_data = ans_json
      return ans_json
   

    def split_data(self):
      self.__clear_splited_data()
      for one_place_data in self.ori_data:
        indicative_section = one_place_data[0] # 0節指示節
        identificative_section = one_place_data[1] # 1節識別説
        data_descriptive_section = one_place_data[2] # 3節資料記述節
        data_section = one_place_data[3] # 4節資料節
        end_section = one_place_data[4] # 5節終端節

        # print("#### 0節 ####")
        # pprint(indicative_section)
        # print("#### 1節 ####")
        # pprint(identificative_section)
        # print("#### 2節 ####")
        # pprint(data_descriptive_section)
        # print("#### 4節 ####")
        # pprint(data_section)
        # print("#### 5節 ####")
        # pprint(end_section)

        self.indicative_sections.append(indicative_section)
        self.identificative_sections.append(identificative_section)
        self.data_descriptive_sections.append(data_descriptive_section)
        self.data_sections.append(data_section)
        self.end_sections.append(end_section)


    def get_date(self):
      X_REPEAT_INDEX = 6
      LATITUDE_IDX, LONGITUDE_IDX = 2, 3 
      YEAR_IDX, MONTH_IDX, DAY_IDX, HOUR_IDX, MIN_IDX = range(0, 5)
      Y_REPEAT_INDEX = 8
      ALTITUDE_IDX = 0
      WIND_EW_IDX, WIND_NS_IDX, WIND_VERTICAL_IDX = 2, 3, 4

      x_index = X_REPEAT_INDEX + 1
      for data_section in self.data_sections:
        data_section = data_section[2][0]
        latitude = data_section[LATITUDE_IDX]
        longitude = data_section[LONGITUDE_IDX]

        x_repeat = data_section[x_index + X_REPEAT_INDEX]
        print(f"北緯:{latitude}, 東経:{longitude}")

        y_index = 0
        for i in range(x_repeat):
          year = data_section[x_index + YEAR_IDX]
          month = data_section[x_index + MONTH_IDX]
          day = data_section[x_index + DAY_IDX]
          hour = data_section[x_index + HOUR_IDX]
          min = data_section[x_index + MIN_IDX]

          print(f"{year}/{month}/{day} {hour}:{min}")
          # print(latitude, longitude, year, month, day, hour, min)

          y_repeat = data_section[x_index + Y_REPEAT_INDEX]
          y_index += x_index + Y_REPEAT_INDEX
          for j in range(y_repeat):
            altitude = data_section[y_index + ALTITUDE_IDX]
            wind_ew = data_section[y_index + WIND_EW_IDX]
            wind_ns = data_section[y_index + WIND_NS_IDX]
            wind_vertical = data_section[y_index + WIND_VERTICAL_IDX]

            print(f"高度: {altitude}, 東西: {wind_ew}m/s, 南北: {wind_ns}m/s, 鉛直: {wind_vertical}")
            y_index += 6

          x_index = y_index




if __name__ == "__main__":
  bu = BufrConverter()
  bu.convert()
  bu.split_data()
  bu.get_date()
