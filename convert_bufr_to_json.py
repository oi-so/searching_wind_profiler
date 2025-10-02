
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import subprocess

class bufrToJson:

	

	def __init__(self, bufr_file_path):

		self.bufr_bin_string = None
		self.bufr_section_0 = None
		self.bufr_section_1 = None
		self.bufr_section_3 = None
		self.bufr_section_4 = None
		self.bufr_section_5 = None

		self.expanded_section_4 = None

		self.wind_datetime_format = [12,4,6,5,6,5,12,8]
		self.wind_data_format = [15,8,13,13,13,8]

		self.left_section_4 = None

		self.kawaguchiko_latitude = 35.5
		self.kawaguchiko_longitude = 138.76

		self.current_latitude = None
		self.current_longitude = None

		
		with open(bufr_file_path, 'rb') as f:
			bufr_file_raw_data = f.read()
			self.bufr_bin_string = ''.join(f'{byte:08b}' for byte in bufr_file_raw_data)[144:]
			


	def convert_bin_to_int_with_minus(self, bin):

		if len(bin) != 13:
			return int(bin, 2)
		elif bin == "1111111111111":
			return "NaN"
		else:
			return (int(bin, 2) - 4096)


	def split_by_sections(self):
		
		self.bufr_section_0 = self.bufr_bin_string[0:64]
		self.bufr_section_1 = self.bufr_bin_string[64:240]
		self.bufr_section_3 = self.bufr_bin_string[240:680]
		self.bufr_section_4 = self.bufr_bin_string[680:-32]
		self.bufr_section_5 = self.bufr_bin_string[-32:]
		
	
	def expand_section_4(self, flg):

		repeat_area_x = None
		num_of_repeat_x = None
		if flg == True:
			try:
				repeat_area_x = self.left_section_4[75:]
				num_of_repeat_x = int(self.left_section_4[67:75], 2)
				self.current_latitude = (int(self.left_section_4[17:32], 2)-9000)/100
				self.current_longitude = (int(self.left_section_4[32:48], 2)-18000)/100
			except:
				return "No Data"
		else:
			repeat_area_x = self.bufr_section_4[107:]
			num_of_repeat_x = int(self.bufr_section_4[99:107], 2)
			self.current_latitude = (int(self.bufr_section_4[49:64], 2)-9000)/100
			self.current_longitude = (int(self.bufr_section_4[64:80], 2)-18000)/100

		

		

		splited_by_x_list = []

		next_x_area = repeat_area_x

		for i in range(num_of_repeat_x):
			datetime = next_x_area[:58]
			num_of_repeat_y = int(datetime[-8:], 2)
			splited_by_x_list.append(next_x_area[:58+num_of_repeat_y*70])
			next_x_area = next_x_area[58+(num_of_repeat_y*70):]
			if True:
				if i == (num_of_repeat_x-1):
					self.left_section_4 = next_x_area

		expanded_wind_data = []

		for i in splited_by_x_list:
			wind_data_by_layer = []
			datetime = i[:58]
			just_wind_data = i[58:]
			num_of_repeat_y = int(datetime[-8:], 2)
			next_wind_data = just_wind_data
			for j in range(num_of_repeat_y):
				wind_data_by_layer.append(next_wind_data[:70])
				next_wind_data = next_wind_data[70:]
			expanded_wind_data.append([datetime, wind_data_by_layer])

		self.expanded_section_4 = expanded_wind_data

	def translate_section_4(self, y, m, d):
		translated_section_4 = []
		for each_data in self.expanded_section_4:
			datetime_bin = each_data[0]
			translated_datetime = []
			next_datetime_bin = datetime_bin
			for i in self.wind_datetime_format:
				translated_datetime.append(next_datetime_bin[:i])
				next_datetime_bin = next_datetime_bin[i:]
			translated_datetime = list(map(int, translated_datetime, [2,2,2,2,2,2,2,2]))
			translated_section_4.append([translated_datetime])
		
		for i in range(len(self.expanded_section_4)):
			wind_data_by_layer = self.expanded_section_4[i][1]
			translated_wind_data_by_layer = []
			for each_layer in wind_data_by_layer:
				translated_wind_data = []
				next_wind_data = each_layer
				for j in self.wind_data_format:
					translated_wind_data.append(next_wind_data[:j])
					next_wind_data = next_wind_data[j:]
				translated_wind_data = list(map(self.convert_bin_to_int_with_minus, translated_wind_data))
				translated_section_4[i].append(translated_wind_data)
		if self.current_latitude == self.kawaguchiko_latitude and self.current_longitude == self.kawaguchiko_longitude:
			output = []
			output_child = []

			for i in translated_section_4:
				for j in range(len(i)):
					new_i = list(map(lambda x: 114514 if x == "NaN" else x, i[j]))
					if j != 0:
						new_i[2] = new_i[2]/10 if new_i[2] != 114514 else new_i[2]
						new_i[3] = new_i[3]/10 if new_i[3] != 114514 else new_i[3]
						new_i[4] = new_i[4]/100 if new_i[4] != 114514 else new_i[4]
					output_child.append(new_i)
				output.append(output_child)
				output_child = []
			json_file_name = "-".join(list(map(str, output[0][0][:4])))
			subprocess.run(["mkdir", f"final_work_dir/converted_jsons/{y}_{m:02}_{d:02}_winprof/"])
			with open(f"final_work_dir/converted_jsons/{y}_{m:02}_{d:02}_winprof/"+json_file_name+".json", "w", encoding="utf-8") as f:
				json.dump(output, f, ensure_ascii=False, indent=4)

		

		
		

def main(path, y, m, d):
	
	bufr = bufrToJson(path)
	bufr.split_by_sections()
	bufr.expand_section_4(False)
	bufr.translate_section_4(y, m, d)
	ret = bufr.expand_section_4(True)
	if ret == "No Data":
		return
	bufr.translate_section_4(y, m, d)
	ret = bufr.expand_section_4(True)
	if ret == "No Data":
		return
	bufr.translate_section_4(y, m, d)
	