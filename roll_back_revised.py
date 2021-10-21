from bs4 import BeautifulSoup
import pandas as pd
import os
import glob
import shutil
import pyodbc
import mysql.connector
import datetime
# define fixed variable

user='admin'
file_source=r'\\ppr-cen-vinod\NPAG BACKUP\Test\\'

try:
    cnxn=pyodbc.connect("DRIVER={MySQL ODBC 8.0 Unicode Driver}; SERVER=localhost;DATABASE=ashu;UID=root;PASSWORD=123456;")
    cursor=cnxn.cursor()
except Exception as Ex:
    ofile = open("Master_Log.txt", "a+")
    outLine = 'ERROR in establishing DB connection: ' + str(Ex) + ' at '  \
              + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()
    exit()

try:
    df_job_id = pd.read_sql("SELECT MAX(job_id) AS max FROM cim_job_status", cnxn)
    job_id = int(df_job_id['max'])
    #Roll Back
    job_status = pd.read_sql("SELECT job_id,status FROM cim_job_status WHERE job_id = (SELECT MAX(job_id) FROM cim_job_status)", cnxn)
    status = list(job_status['status'])
    status_1 = [x for x in status if "job failed" in x]
    status_2 = [x for x in status if "rollback complete" in x]
    if job_status.shape[0] == 1:
        cursor.execute("DELETE FROM cim_feeder_list WHERE job_id = " + str(job_id))
        cursor.execute("DELETE FROM cim_master WHERE job_id = " + str(job_id))
        # insert "rollback complete"
        now = datetime.datetime.now()
        sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")
        feeder_value = [job_id,
                        sys_datetime,
                        user,
                        'rollback complete'
                        ]
        str_value = str(feeder_value).replace("[", "").replace("]", "")
        cursor.execute(
            "INSERT INTO cim_job_status (job_id,insert_date_time,user_name,status) values(" + str_value + ")")
        cnxn.commit()
    elif len(status_1) == 1:
        if len(status_2) == 0:
            cursor.execute("DELETE FROM cim_feeder_list WHERE job_id = " + str(job_id))
            cursor.execute("DELETE FROM cim_master WHERE job_id = " + str(job_id))
            # insert "rollback complete"
            now = datetime.datetime.now()
            sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")
            feeder_value = [job_id,
                            sys_datetime,
                            user,
                            'rollback complete'
                            ]
            str_value = str(feeder_value).replace("[", "").replace("]", "")
            cursor.execute("INSERT INTO cim_job_status (job_id,insert_date_time,user_name,status) values(" + str_value + ")")
            cnxn.commit()


    #START NEW JOB
    now = datetime.datetime.now()
    job_id += 1
    sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")
    feeder_value = [job_id,
                    sys_datetime,
                    user,
                    'job started'
                    ]
    str_value = str(feeder_value).replace("[", "").replace("]", "")
    cursor.execute("INSERT INTO cim_job_status (job_id,insert_date_time,user_name,status) values(" + str_value + ")")
    cnxn.commit()

except Exception as Ex:
    ofile = open("Master_Log.txt", "a+")
    outLine = 'ERROR in initiating job: ' + str(Ex) + ' at '  \
              + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()
    exit()

try:
    ofile = open("CIM_Log_" + str(job_id) + ".txt", "w")
    outLine = "Process started at " + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()
except Exception as Ex:
    ofile = open("Master_Log.txt", "a+")
    outLine = 'ERROR in creating job log file: ' + str(Ex) + ' at '  \
              + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()
    exit()

try:
    # Read files from directory
    file_name=os.listdir(file_source)

    # Identify Exchange files from directory
    exch_model_file_list=[]

    for file in file_name:
        if 'ExchangedModel' in file:
            exch_model_file_list.append(file)
    print(len(exch_model_file_list))
    df_old_exch_file_name=pd.read_sql("SELECT * FROM cim_master",cnxn)
    old_exch_file_name_list=list(df_old_exch_file_name['exch_file_name'])
    new_exch_file_name_list=list(set(exch_model_file_list)-set(old_exch_file_name_list))
    print(len(new_exch_file_name_list))
    # to copy exchange file into specific folder


    for new_file in new_exch_file_name_list:
        file = open(file_source+ new_file,'r')
        content=file.read()
        soup = BeautifulSoup(content,"lxml-xml")
        xml_file_name = soup.find_all('CIMURL')
        gml_file_name = soup.find_all('GMLURL')
        job_name = soup.find_all('JobName')
        date_time = soup.find_all('DateTime')
        now = datetime.datetime.now()
        sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")

        master_value = [new_file.split('Information_')[1],
                new_file,
                xml_file_name[0].get_text(),
                gml_file_name[0].get_text(),
                job_name[0].get_text(),
                date_time[0].get_text().split('T')[0],
                date_time[0].get_text().split('T')[1],
                sys_datetime,user,job_id
                ]
        str_value=str(master_value).replace("[","").replace("]","")
        cursor.execute("INSERT INTO cim_master (patch_name,exch_file_name,xml_file_name,gml_file_name,job_name,date,time,insert_date_time,user_name,job_id) values(" + str_value + ")")
        cnxn.commit()

        df_pid = pd.read_sql("SELECT MAX(patch_id) AS max FROM cim_master", cnxn)
        patch_id = int(df_pid['max'])

        xml_file = open(file_source + xml_file_name[0].get_text(), 'r')
        content = xml_file.read()
        soup = BeautifulSoup(content, "lxml-xml")
        value = soup.find_all('cim:Name')
        circuit_id=[]
        for j in range(len(value)):
            x = value[j].find_all('cim:Name.NameType', {'rdf:resource': '#NameType_Circuit_ID'})
            if len(x) == 0:
                pass
            else:
                y = value[j].find_all('cim:Name.name')[0].get_text()
                circuit_id.append(y)
        circuit_id= list(set(circuit_id))
        mv_circuit = [k for k in circuit_id if '-' in k]
        lv_circuit = [k for k in circuit_id if not "-" in k]
        for circuit in mv_circuit:
            feeder_value = [circuit,
                            'mv',
                            patch_id,
                            user,
                            sys_datetime,
                            job_id
                            ]
            str_value = str(feeder_value).replace("[", "").replace("]", "")
            cursor.execute("INSERT INTO cim_feeder_list (circuit_id,type,patch_id,user_name,insert_date_time,job_id) values(" + str_value + ")")
            cnxn.commit()
        for circuit in lv_circuit:
            feeder_value = [circuit,
                            'lv',
                            patch_id,
                            user,
                            sys_datetime,
                            job_id
                            ]
            str_value = str(feeder_value).replace("[", "").replace("]", "")
            cursor.execute("INSERT INTO cim_feeder_list (circuit_id,type,patch_id,user_name,insert_date_time,job_id) values(" + str_value + ")")
            cnxn.commit()

    sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")
    feeder_value = [job_id,
                    sys_datetime,
                    user,
                    'job completed'
                    ]
    str_value = str(feeder_value).replace("[", "").replace("]", "")
    cursor.execute("INSERT INTO cim_job_status (job_id,insert_date_time,user_name,status) values(" + str_value + ")")
    cnxn.commit()

    cnxn.commit()
    cursor.close()

    ofile = open("CIM_Log_" + str(job_id) + ".txt", "a+")
    outLine = "Process completed at " + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()

except Exception as Ex:
    ofile = open("CIM_Log_" + str(job_id) + ".txt", "a+")
    outLine = 'ERROR: ' + str(Ex)
    ofile.write(outLine)
    outLine = "Program terminated at" + str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")) + ".\n"
    ofile.write(outLine)
    ofile.close()
    sys_datetime = now.strftime("%d/%m/%Y %H:%M:%S")
    feeder_value = [job_id,
                    sys_datetime,
                    user,
                    'job failed'
                    ]
    str_value = str(feeder_value).replace("[", "").replace("]", "")
    cursor.execute("INSERT INTO cim_job_status (job_id,insert_date_time,user_name,status) values(" + str_value + ")")
    cnxn.commit()


# cnxn=pyodbc.connect("DRIVER={MySQL ODBC 8.0 Unicode Driver}; SERVER=localhost;DATABASE=ashu;UID=root;PASSWORD=123456;")
# cursor=cnxn.cursor()
# cursor.execute('DELETE FROM cim_master')
# cnxn.commit()
# cursor.close()