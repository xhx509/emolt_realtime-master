# Weekly-process

Main “weekly_project.drawio”:https://app.diagrams.net/#G1Hv6e2M8L7ib0npZYsgmGDd3ngD3JXgra

Weekly_Project.zip include all of code and parameter files for running weekly process!!!

weekly-process include check_csv.py,process.py,create_modules_dictionary.py and diff_clim_obs.py:

      check_csv.py:check the data and fix it
            repositories of data:https://66.114.154.52:8443/smb/file-manager/list#   home directory/anno_ftp/Matdata
      process.py:get statistics.csv,emolt_no_telemetry.csv,emolt_raw.csv and emolt_QCed_telemetry_and_wified.csv
      create_modules_dictionary.py:update dictionary.json
      diff_clim_obs.py:get weekly.html
The parameter of Weekly process should include raw_data_name.txt , telemetry_status.csv and dictionary.json 3 parts:
      
      raw_data_name.txt:If wouldn't add new vessel,we don't need to change
      telemetry_status.csv: Before run WeeklyProcess,downloaded from https://docs.google.com/spreadsheets/d/1uLhG_q09136lfbFZppU2DU9lzfYh0fJYsxDHUgMB1FM/edit?ts=5ba8fe2b#gid=0 as a "tab-delimited comma-delimited and "-delimited" csv file
      dictionary.json: receive from Jim every week
Nov 14,2019:

      update match_tele_raw/raw_tele_modules.py to count number boats in statistics.csv by adding the key of tele_total_num

Nov 25,2019:

      update match_tele_raw/raw_tele_modules.py:By creating emolt_raw.csv to compares with emolt.dat and got the absent of  emolt.dat named emolt_no_telemetry.csv
      change start_time to datetime.now-timedelta(weeks=1)
Dec 10,2019:

      create lack_data.txt to store the problem files before the modules of check_format data and match_tele_raw run

Jan 2020:

      combine_vessels_hour_data.py:Contact each vessel's raw data to one file named likes 'Virginia_Marise_hours.csv'.
      plot_each_vessel_hours.py:Plot each vessels' raw data of hours

Feb 2020:

    Change the time to UTC
    Append emolt_no_telemetry every week
    the telemetry data have wrong lat and lon,but raw data have right lat and lon.we will compare them and put right raw data in emolt_no_telemetry


