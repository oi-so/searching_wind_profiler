from eccodes import codes_bufr_new_from_file, codes_get, codes_set, codes_release


FOLDER_NAME = "data/IUPC00_COMP_202501010026187_010_48431.send"
with open(f'{FOLDER_NAME}/IUPC41_RJTD_010000_202501010016181_001.send', 'rb') as f:
    bufr = codes_bufr_new_from_file(f)
    if bufr is not None:
        codes_set(bufr, 'unpack', 1)
        print(codes_get(bufr, 'dataCategory'))
        codes_release(bufr)

