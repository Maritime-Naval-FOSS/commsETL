#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import getopt
import os.path
import xlrd
import cx_Oracle
import string

#from datetime import datetimnvoice_num,e
import time
import datetime

import unicodedata
from_chars = '�����������������������������������������������������������ٶ���ڼ�ۿ'
to_chars =   'abgdezh8iklmn3oprsstufxywaehiiiouuuwABGDEZH8IKLMNJOPRSTYFXCWAEHIIOUUW'
translation_table = string.maketrans( from_chars, to_chars )

def getUnicodeStrVal(ws, row, col):
    uni_val = ws.cell(rowx=row, colx=col).value
    std_str = unicodedata.normalize('NFKD', uni_val).encode('iso8859_7', 'ignore')
    return string.translate( std_str , translation_table )

def getStrVal(ws, row, col):
#   print " ("+str(row)+" , "+str(col)+") ", \
#       ws.cell(rowx=row, colx=col).ctype, type(ws.cell(rowx=row, colx=col).value), \
#       ws.cell(rowx=row, colx=col).value
    value = ws.cell(rowx=row, colx=col).value
    try:
        string = str(value)
    except UnicodeEncodeError:
        string = ""
        count = 0
        for i in range(0, len(value)):
            try:
                string += str(value[i])
            except UnicodeEncodeError:
                count += 1
            except:
                raise
        debug(2, "UnicodeEncodeError @ ("+str(row)+", "+str(col)+") `"+string+"` #"+str(count)+" ignored.\n")
    except:
        raise
    return string.strip()

def getStrDate(wb, ws, row, col):
    return datetime.datetime(*xlrd.xldate_as_tuple(ws.cell_value(row, col), wb.datemode)).strftime('%Y-%m-%d')

def getStrTime(wb, ws, row, col):
    floatVal = ws.cell_value(row, col)
    try:
        values = xlrd.xldate_as_tuple(floatVal, wb.datemode)
    except xlrd.xldate.XLDateAmbiguous:
        values = xlrd.xldate_as_tuple(floatVal+366, wb.datemode)

    return str(values[2]*24+values[3])+":"+str(values[4])+":"+str(values[5])

_files, _debug = 0, -1

def execute(cursor, insert, data):
    data_tuple = tuple(data)
    try:
        cursor.execute( str( insert % data_tuple ) )
#        debug(1, ".", False)
        return True
#    except Error e:
    except Exception, inst:
        if str(inst.__class__).find("IntegrityError")==-1:
           debug(2, "\n"+insert % data_tuple+"\n")
           raise inst
        else:
#            debug(1, "!", False)
            return False

def insert_xls(data_tuples, validate_only):

    insert_data = u"""
        insert into vodafone_fee  (
            issue_date, caller_id, bill_id, contract_type, monthly_fee, discount_fee,
            call_charge, discount_charge, discount_misc, mobile_tax, untaxed_amount,
            vated_amount, vat_amount, total_amount, call_count, call_duration, call_volume
        ) values ("""

    connection = cx_Oracle.connect(
        "comms/<PASS>//260.270.280.290:1521/orcl" )
    insert_data += u"""
            to_date('%s', 'YYYY-MM-DD'),
            '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',
            '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'
        )"""
#           STR_TO_DATE(':Starttime', '%H:%i:%s %e %b %Y'),

    records, errors, rows = 0, 0, 0
    if len(data_tuples)>0:
        cursor = connection.cursor ()
        debug(1, "Inserting DATA\n")
        for f in data_tuples:
            record = f # full record
            debug(5, "record {"+(",".join(record))+"}\n")
            if execute(cursor, insert_data, record):
                records+=1
            else:
                errors+=1
#            if ( (records+errors) % 100 ) == 0:
#                debug(0, "\n", False)
#                cursor.execute( "commit" )

        rows = 0
        if not validate_only:
            cursor.execute( "commit" )
            rows = records
        else:
            debug(2, "NOT commit executed\n")

        cursor.close ()
        connection.close ()

    debug(1, "DONE.\n")
    return records, errors, rows

def parse_xls(filename):
    data_tuples, bill_num = [], '0000000000'
   #columns_o_i = ( 3, 4, 5, 6, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25 )
    columns_o_i = ( 2, 3, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 )

    wb = xlrd.open_workbook(filename)
    for ws in wb.sheets():
        row = 4
        col = 0

#       ignore first dummy rows...
#       while True:
#           val = getStrVal(ws, row, col)
#           debug(6, "Comparing ["+ws.name+"] against ["+val+"] @ ("+str(row)+" , "+str(col)+")\n")
#           row += 1

        while (row < ws.nrows):
#           if (getStrVal(ws, row, 1).isdigit()):
            record = []
            for col in columns_o_i:
                if col==6:
                    record.append(getUnicodeStrVal(ws, row, col))
                elif col==2:
                    record.append(getStrDate(wb, ws, row, col))
                elif col==21:
                    record.append(getStrTime(wb, ws, row, col))
                else:
                    record.append(getStrVal(ws, row, col))
#               debug(5, "record ["+str(record)+"]\n")
            data_tuples.append( record )
            row += 1
            debug(4, "Got ["+str(row)+"] / "+str(ws.nrows)+"-=> "+str(len(data_tuples))+" records\n")

    if row>0:
         bill_num=data_tuples[0][2]
    return data_tuples, bill_num


def insert_csv(data_tuples, received, source, validate_only, bill_num):

    if received == None:
        received = 'trunc(sysdate)'
    else:
        received = "to_date('" + received + "', 'YYYY-MM-DD')"

    truncate_table = "truncate table comm_imp_vodafone"

    insert_data = """
        insert into comm_imp_vodafone  (
            caller_id, call_date, call_type, roaming_info, called_apn,
            called_net, provider, paid_duration, call_duration, call_volume, call_charge, bill_id
        ) values ("""

    connection = cx_Oracle.connect(
        "comms/<PASS>//260.270.280.290:1521/orcl" )
    insert_data += """
            %s, to_date(%s||' '||%s, 'DD/MM/YYYY HH24:MI:SS'),
            %s, %s, %s, %s, %s, %s, %s, replace(%s,',','.'), replace(%s,',','.'),
           ':bill_id'
        )""".replace(":bill_id", bill_num)
#           STR_TO_DATE(':Starttime', '%H:%i:%s %e %b %Y'),

    insert_log = """
insert into vodafone_log
select v.caller_id, v.call_date, v.call_type, v.roaming_info, v.called_apn,
       v.called_net, v.call_duration, v.call_volume, v.call_charge,
       systimestamp timestamp, bill_id
  from comm_imp_vodafone v
"""

    # BUSINESS TODO - _fee table should now be used NOT _log
    insert_final = """
insert into comm_charges
select '*EX*' vesselcode, 110 provider,
       :received received, 'MOB' reference,
       Null invoice, to_char(max(q.call_date), 'MONYYYY') period,
       to_date(extract(year from min(q.call_date))||'-'||extract(month from min(q.call_date))||'-01', 'YYYY-MM-DD') date_from,
       last_day(max(q.call_date)) date_to,
       nvl( (select t.id from comm_traffic_types t where upper(t.code) = q.unit), 0) traffic_type,
       decode(unit, 'MB', sum(q.call_volume), round(sum(q.call_duration)/60,2)) traffic, 0 in_bundle,
       sum(q.call_charge) amount, 'EUR' currency, count(1) comments,
       systimestamp
  from (
    select v.caller_id, v.call_date, v.call_type,
           decode(v.call_type,
                  'Dedomena me ogkoxrewsh', 'MB', 'Dedomena Periagwghs (Roaming)', 'MB',
                  'Mhnumata MMS', 'MB', 'Loipes Yphresies dedomenwn', 'MB',
                  case when nvl(v.call_volume,0)>0 and nvl(v.call_duration,0)=0 then 'MB' else 'MIN' end
                 ) unit,
           v.roaming_info, v.called_apn, v.called_net,
           v.call_duration, v.call_volume, v.call_charge
      from comm_imp_vodafone  v
  ) q
 group by  q.unit
    """.replace(":received", received)

    debug(1, "Insert type "+str(source)+" for "+str(received)+"\n")

    records, errors, rows = 0, 0, 0
    if len(data_tuples)>0:
        cursor = connection.cursor ()
        debug(2, "Cleaning DATA - temp\n")
        cursor.execute( truncate_table )
        debug(1, "Inserting DATA - temp\n")
        for f in data_tuples:
#            if   source == "MOB":
#            else:
            record = f # full record
            debug(5, "record {"+(",".join(record))+"}\n")
            if execute(cursor, insert_data, record):
                records+=1
            else:
                errors+=1
#            if ( (records+errors) % 100 ) == 0:
#                debug(0, "\n", False)
#                cursor.execute( "commit" )

        if not validate_only:
            debug(1, "Inserting DATA - final\n")
            cursor.execute( insert_final )
            rows = cursor.rowcount
            debug(1, "Migrating temp data to perm table\n")
            cursor.execute( insert_log )
            debug(2, "Cleaning DATA - temp\n")
            cursor.execute( truncate_table )
        else:
            debug(2, "NOT cleaning DATA - temp\n")

        cursor.execute( "commit" )
        cursor.close ()
        connection.close ()

    debug(1, "DONE.\n")
    return records, errors, rows

def parse_csv(filename):
    firstlines = {}
    firstlines['MOB'] = 'Ari8mos_Sundromhth;Hmeromhnia;Wra;Eidos_Klhshs;Xwra_Periagwghs;Ari8mos_Klhshs_APN;Diktuo;Paroxos Y.P.P.;Diarkeia;Timologh8eisa diarkeia;Ogkos_Dedomenwn_MB;A3ia_pro_FPA'

    """ directory parsing disabled

    if os.path.isdir(filename)
        debug(0, "***DIR*** listing directory '"+filename+"'\n")
        global _files
        _files-=1
        for entry in os.listdir(filename):
            entry = os.path.join(filename, entry)
            if os.path.isfile(entry):
                data = parse(entry)
                data_tuples.extend(tuple(data))
                _files+=1
    """

    if not os.path.isfile(filename):
        return data_tuples

    debug(0, "***INIT*** parsing file '"+filename+"'\n")

    file = open(filename)
    line = string.translate( file.readline(), translation_table)

    source = 'MOB' #os.path.basename(filename)[:3]
    firstline = firstlines[source]
    if firstline[-1]==';': firstline=firstline[:-1]
    headers = firstline.split(";")

    lines = 0
    while len(line)!=0 and not line.startswith(firstline):
        lines+=1
        line = string.translate( file.readline(), translation_table)
    debug(2, "***TOP*** skipping lines '"+str(lines)+"'\n")

    data_tuples = []
    record = []
    line = string.translate( file.readline(), translation_table)
    lines = 0
    while len(line)!=0:
        line = line.replace("\n","").replace("\r","")
        if len(line)==0:
            break
        elements = line.split(";")
#        if len(elements)-len(headers)==2:
#            """ remove the two known empty columns R and S"""
#            elements = elements[:17] + elements[19:]
        if len(elements)>len(headers):
            """ truncate to the number of headers"""
            elements = elements[:len(headers)]
        elif len(elements)<len(headers):
            """ add some empty ones at the end... """
            elements += [""]*(len(headers)-len(elements))

        """
        try:
# 2.6 only / not 2.4    converter = datetime.strptime(
#                           elements[-1].strip(), "%H:%M:%S %d %b %Y")
            converter = datetime.datetime(
                *(time.strptime(elements[0].strip(),
                "%Y/%m/%d %H:%M:%S")[0:6]))
            elements[0] = converter.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            elements[0] = "0001-01-01 00:00:00"

        try:
# 2.6 only / not 2.4    converter = datetime.strptime(
#                           elements[-1].strip(), "%H:%M:%S %d %b %Y")
            converter = datetime.datetime(
                *(time.strptime(elements[1].strip(),
                "%H:%M:%S")[0:6]))
            elements[1] = converter.strftime("%H:%M:%S")
        except ValueError:
            elements[1] = "00:00:00"
        """

        for i in range(len(headers)):
# 2.6 only / not 2.4
#            elements[i+2] = \
#                elements[i+2]='Null' if len(elements[i+2].strip())==0 \
#                else "'"+elements[i+2]+"'"
            debug(6, str(lines)+' **ELEMENT***_'+elements[i]+'_***ELEMENT***'+str(type(elements[i]))+'\n');
            elements[i] = elements[i].strip()
            if len(elements[i])==0:
                elements[i]='Null'
            else:
                if elements[i][0]=='"':
                    elements[i]=elements[i][1:]
                if elements[i][-1]=='"':
                    elements[i]=elements[i][:-1]
                if len(elements[i])==0 or elements[i]=='-0':
                    elements[i]='Null'
                elif i==9:
                    hms = elements[i].split(':')
                    elements[i] = str ( ( int(hms[0]) * 60 + int(hms[1]) ) * 60 + int(hms[2] ) )
            if elements[i]!='Null':
                elements[i]="'"+elements[i]+"'"
            debug(6, str(lines)+' **element***_'+elements[i]+'_***element***'+str(type(elements[i]))+'\n');

#        debug(2, line+"\n")
#        debug(2, ",".join( [e for e in elements] )+"\n" )
        record.append(elements)

        lines += 1
        data_tuples += record
        record = []
        line = string.translate( file.readline(), translation_table)
    file.close()
    return data_tuples, source

def usage(w, i=0):
    w = os.path.basename(w)
    print \
"""Usage:\t%s File(s)ToBeImported ...
\tuse -d to turn on debugging (multiple times for more debug info) and
\tuse -r or --received to set the received date (if undefined current date will by used)
\tuse -v or --validate to only validate the files
\tuse -h or --help for help (.i.e. this message)
\tfor example %s -r 2013-02-05 *.csv""" % (w, w)
    sys.exit(i)

def debug(level, message, prefix=True):
    if _debug>=level:
        if prefix:
            message = "["+str(level)+"] "+message
        sys.stdout.write(message)

def main(whoami, argv):
    global _files, _debug
    validate_only, _files, _debug, bill_no = False, 0, -1, ''
    received = None
    try:
        opts, args = getopt.getopt(argv, "r:hvd", ["received=", "help", "validate"])
    except getopt.GetoptError:
        usage(whoami, 2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage(whoami)
        elif opt in ("-r", "--received"):
            received = arg
        elif opt in ("-v", "--validate"):
            validate_only = True
        elif opt == "-d":
            _debug += 1
    debug(0, "Debug level at "+str(_debug)+"\n")
    passed, failed, dbrows = 0, 0, 0

    #bring the .XLS files in front to make sure we have the bill number ready for CSV importation
    head, tail  = [], []
    for arg in args:
        if arg.endswith('.xls'):
            head.append(arg)
        else:
            tail.append(arg)
    args = head + tail

    for arg in args:
        if os.path.exists(arg):
            data, source, records, errors, rows = [], [], -1, -1, -1
            file_type = os.path.splitext(arg)[1].lower()

            if file_type == '.csv':
                data, source = parse_csv(arg)
            elif file_type == '.xls':
                data, bill_no = parse_xls(arg)
            if file_type in ('.csv', '.xls'):
                debug(0, "***DONE*** ["+file_type+"] reading "+str(len(data))+
                    " data records. ["+bill_no+"]\n")

            if file_type == '.csv':
                if bill_no != '':
                    records, errors, rows = insert_csv(data, received, source, validate_only, bill_no)
                else:
                    debug(0, "Skipping file "+arg+" : No BILL NUMBER available (parse an XLS first.\n")
            elif file_type == '.xls':
                records, errors, rows = insert_xls(data, validate_only)

            if file_type in ('.csv', '.xls'):
                debug(0, "***DONE*** ["+file_type+"] inserting "+str(records)+
                    " records to database (with "+str(errors)+
                    " errors) resulted in "+str(rows)+" new rows\n")

            if file_type in ('.csv', '.xls'):
                if records>0 and not validate_only:
                    import shutil
                    shutil.move(arg,
                        os.path.join(os.path.dirname(arg),'parsed',os.path.basename(arg)))
                passed+=records
                failed+=errors
                dbrows+=rows
                _files+=1
            else:
                debug(0, "Skipping file "+arg+" because it is of not imporable type...["+file_type+"]\n")
        else:
            from os import getcwd
            debug(0, "Skipping file "+arg+" because it does not exist...["+getcwd()+"]\n")
    debug(0, "***FINISHED*** successfully parsed " +str(_files)+
        " files, inserted "+str(dbrows)+" rows by parsing "+str(passed)+
        " and ignored "+str(failed)+" records!\n")

if __name__ == '__main__':
    main(sys.argv[0], sys.argv[1:])
